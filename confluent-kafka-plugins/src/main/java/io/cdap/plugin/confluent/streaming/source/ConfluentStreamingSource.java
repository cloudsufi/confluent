/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.confluent.streaming.source;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingStateHandler;
import io.cdap.plugin.batch.source.KafkaPartitionOffsets;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.confluent.source.ConfluentDStream;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Confluent Kafka Streaming source.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(ConfluentStreamingSource.PLUGIN_NAME)
@Description("Confluent Kafka streaming source.")
public class ConfluentStreamingSource extends StreamingSource<StructuredRecord> implements StreamingStateHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ConfluentStreamingSource.class);
  private static final Gson gson = new Gson();
  public static final String PLUGIN_NAME = "Confluent";

  private final ConfluentStreamingSourceConfig conf;

  public ConfluentStreamingSource(ConfluentStreamingSourceConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    conf.validate(collector);
    Schema schema = getOutputSchema(collector);
    stageConfigurer.setOutputSchema(schema);

    if (conf.getMaxRatePerPartition() != null && conf.getMaxRatePerPartition() > 0) {
      Map<String, String> pipelineProperties = new HashMap<>();
      pipelineProperties.put("spark.streaming.kafka.maxRatePerPartition", conf.getMaxRatePerPartition().toString());
      pipelineConfigurer.setPipelineProperties(pipelineProperties);
    }
    pipelineConfigurer.createDataset(conf.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    conf.validate(collector);
    Schema outputSchema = getOutputSchema(collector);
    collector.getOrThrowException();

    context.registerLineage(conf.referenceName);
    JavaInputDStream<ConsumerRecord<Object, Object>> javaInputDStream = ConfluentStreamingSourceUtil
      .getConsumerRecordJavaDStream(context, conf, outputSchema, collector, getStateSupplier(context));

    JavaDStream<StructuredRecord> javaDStream;
    javaDStream = ConfluentStreamingSourceUtil.getStructuredRecordJavaDStream(javaInputDStream,
      new ConfluentStreamingSourceUtil.RecordTransform(conf, outputSchema));

    if (conf.getSchemaRegistryUrl() != null) {
      ConfluentStreamingSourceUtil.AvroRecordTransform transform =
        new ConfluentStreamingSourceUtil.AvroRecordTransform(conf, outputSchema);
      javaDStream = ConfluentStreamingSourceUtil.getStructuredRecordJavaDStream(javaInputDStream, transform);
    }

    if (!context.isStateStoreEnabled()) {
      // Return the serializable Dstream in case checkpointing is enabled.
      return javaDStream;
    }

    // Use the DStream that is state aware

    ConfluentDStream confluentDStream = new ConfluentDStream(context.getSparkStreamingContext().ssc(),
                                                             javaInputDStream.inputDStream(),
                                                             ConfluentStreamingSourceUtil
                                                               .getRecordTransformFunction(conf, outputSchema),
                                                             getStateConsumer(context));
    return confluentDStream.convertToJavaDStream();
  }

  private Schema getOutputSchema(FailureCollector failureCollector) {
    if (conf.getSchemaRegistryUrl() == null) {
      return conf.getSchema(failureCollector);
    }
    return inferSchema(failureCollector);
  }

  private Schema inferSchema(FailureCollector failureCollector) {
    try {
      Map<String, Object> options = new HashMap<>();
      options.put("basic.auth.credentials.source", "USER_INFO");
      options.put("basic.auth.user.info", conf.getSchemaRegistryApiKey() + ':' + conf.getSchemaRegistryApiSecret());
      CachedSchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(conf.getSchemaRegistryUrl(), 2, options);
      Schema initialSchema = conf.getSchema(failureCollector);
      List<Schema.Field> newFields = new ArrayList<>();
      boolean keySchemaShouldBeAdded = conf.getKeyField() != null;
      boolean messageSchemaShouldBeAdded = conf.getValueField() != null;
      for (Schema.Field field : initialSchema.getFields()) {
        if (field.getName().equals(conf.getKeyField())) {
          Schema keySchema = fetchSchema(schemaRegistryClient, conf.getTopic() + "-key");
          newFields.add(Schema.Field.of(field.getName(), keySchema));
          keySchemaShouldBeAdded = false;
        } else if (field.getName().equals(conf.getValueField())) {
          Schema valueSchema = fetchSchema(schemaRegistryClient, conf.getTopic() + "-value");
          newFields.add(Schema.Field.of(field.getName(), valueSchema));
          messageSchemaShouldBeAdded = false;
        } else {
          newFields.add(field);
        }
      }
      if (keySchemaShouldBeAdded) {
        Schema keySchema = fetchSchema(schemaRegistryClient, conf.getTopic() + "-key");
        newFields.add(Schema.Field.of(conf.getKeyField(), keySchema));
      }
      if (messageSchemaShouldBeAdded) {
        Schema valueSchema = fetchSchema(schemaRegistryClient, conf.getTopic() + "-value");
        newFields.add(Schema.Field.of(conf.getValueField(), valueSchema));
      }
      return Schema.recordOf(initialSchema.getRecordName(), newFields);
    } catch (IOException | RestClientException e) {
      failureCollector.addFailure("Failed to infer output schema. Reason: " + e.getMessage(), null)
        .withStacktrace(e.getStackTrace());
      throw failureCollector.getOrThrowException();
    }
  }

  private Schema fetchSchema(CachedSchemaRegistryClient schemaRegistryClient, String subject)
    throws IOException, RestClientException {
    SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
    if (schemaMetadata.getSchema().startsWith("\"")) {
      String typeName = schemaMetadata.getSchema().substring(1, schemaMetadata.getSchema().length() - 1);
      return Schema.of(Schema.Type.valueOf(typeName.toUpperCase()));
    }
    return Schema.parseJson(schemaMetadata.getSchema());
  }

  private VoidFunction<OffsetRange[]> getStateConsumer(StreamingContext context) {
    return offsetRanges -> {
      try {
        saveState(context, offsetRanges);
      } catch (IOException e) {
        LOG.warn("Exception in saving state.", e);
      }
    };
  }

  private void saveState(StreamingContext context, OffsetRange[] offsetRanges) throws IOException {
    if (offsetRanges.length > 0) {
      Map<Integer, Long> partitionOffsetMap = Arrays.stream(offsetRanges)
        .collect(Collectors.toMap(OffsetRange::partition, OffsetRange::untilOffset));
      byte[] state = gson.toJson(new KafkaPartitionOffsets(partitionOffsetMap)).getBytes(StandardCharsets.UTF_8);
      context.saveState(conf.getTopic(), state);
    }
  }

  private Supplier<Map<TopicPartition, Long>> getStateSupplier(StreamingContext context) {
    return () -> {
      try {
        return getSavedState(context);
      } catch (IOException e) {
        throw new RuntimeException("Exception in fetching state.", e);
      }
    };
  }

  private Map<TopicPartition, Long> getSavedState(StreamingContext context) throws IOException {
    //State store is not enabled, do not read state
    if (!context.isStateStoreEnabled()) {
      return Collections.emptyMap();
    }

    //If state is not present, use configured offsets or defaults
    Optional<byte[]> state = context.getState(conf.getTopic());
    if (!state.isPresent()) {
      return Collections.emptyMap();
    }

    byte[] bytes = state.get();
    try (Reader reader = new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8)) {
      KafkaPartitionOffsets partitionOffsets = gson.fromJson(reader, KafkaPartitionOffsets.class);
      return partitionOffsets.getPartitionOffsets().entrySet()
        .stream()
        .collect(Collectors.toMap(partitionOffset -> new TopicPartition(conf.getTopic(), partitionOffset.getKey()),
                                  Map.Entry::getValue));
    }
  }
}
