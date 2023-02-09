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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.format.RecordFormats;
import io.cdap.plugin.batch.source.KafkaPartitionOffsets;
import io.cdap.plugin.confluent.common.KafkaHelpers;
import io.cdap.plugin.confluent.source.ConfluentDStream;
import io.cdap.plugin.format.avro.AvroToStructuredTransformer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Util method for {@link ConfluentStreamingSource}.
 * <p>
 * This class contains methods for {@link ConfluentStreamingSource} that require spark classes because during validation
 * spark classes are not available. Refer CDAP-15912 for more information.
 */
final class ConfluentStreamingSourceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ConfluentStreamingSourceUtil.class);
  private static final Gson gson = new Gson();

  private ConfluentStreamingSourceUtil() {
    // no-op
  }

  /**
   * Returns {@link JavaInputDStream} for {@link ConfluentStreamingSource}.
   *
   * @param context   streaming context
   * @param conf      kafka conf
   * @param collector failure collector
   * @param stateSupplier state supplier
   */
  static <K, V> JavaInputDStream<ConsumerRecord<K, V>> getConsumerRecordJavaDStream(
    StreamingContext context, ConfluentStreamingSourceConfig conf, FailureCollector collector,
    Supplier<Map<TopicPartition, Long>> stateSupplier) {
    String pipelineName = context.getPipelineName();
    Map<String, Object> kafkaParams = getConsumerParams(conf, pipelineName);
    Properties properties = new Properties();
    properties.putAll(kafkaParams);
    try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(),
                                                                 new ByteArrayDeserializer())) {
      Map<TopicPartition, Long> offsets = getOffsets(conf, collector, consumer, stateSupplier);
      LOG.info("Using initial offsets {}", offsets);

      return KafkaUtils.createDirectStream(
        context.getSparkStreamingContext(), LocationStrategies.PreferConsistent(),
        ConsumerStrategies.Subscribe(Collections.singleton(conf.getTopic()), kafkaParams, offsets)
      );
    }
  }

  /**
   * Returns {@link JavaDStream} for {@link ConfluentStreamingSource}.
   *
   * @param context   streaming context
   * @param conf      kafka conf
   * @param outputSchema output schema
   * @param collector failure collector
   */
  static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context,
    ConfluentStreamingSourceConfig conf, Schema outputSchema, FailureCollector collector) {
    JavaInputDStream<ConsumerRecord<Object, Object>> javaInputDStream = getConsumerRecordJavaDStream(context, conf,
      collector, getStateSupplier(context, conf));

    if (!context.isStateStoreEnabled()) {
      // Return the serializable DStream in case checkpointing is enabled.
      if (conf.getSchemaRegistryUrl() != null) {
        return javaInputDStream.transform(new AvroRecordTransform(conf, outputSchema));
      } else {
        return javaInputDStream.transform(new RecordTransform(conf, outputSchema));
      }
    }

    // Use the DStream that is state aware

    ConfluentDStream confluentDStream = new ConfluentDStream(context.getSparkStreamingContext().ssc(),
                                                             javaInputDStream.inputDStream(),
                                                             getTransformFunction(conf, outputSchema),
                                                             getStateConsumer(context, conf));
    return confluentDStream.convertToJavaDStream();

  }

  private static VoidFunction<OffsetRange[]> getStateConsumer(StreamingContext context,
                                                              ConfluentStreamingSourceConfig conf) {
    return offsetRanges -> {
      try {
        saveState(context, offsetRanges, conf);
      } catch (IOException e) {
        LOG.warn("Exception in saving state.", e);
      }
    };
  }

  private static void saveState(StreamingContext context, OffsetRange[] offsetRanges,
                                ConfluentStreamingSourceConfig conf) throws IOException {
    if (offsetRanges.length > 0) {
      Map<Integer, Long> partitionOffsetMap = Arrays.stream(offsetRanges)
        .collect(Collectors.toMap(OffsetRange::partition, OffsetRange::untilOffset));
      byte[] state = gson.toJson(new KafkaPartitionOffsets(partitionOffsetMap)).getBytes(StandardCharsets.UTF_8);
      context.saveState(conf.getTopic(), state);
    }
  }

  private static Supplier<Map<TopicPartition, Long>> getStateSupplier(StreamingContext context,
                                                                      ConfluentStreamingSourceConfig conf) {
    return () -> {
      try {
        return getSavedState(context, conf);
      } catch (IOException e) {
        throw new RuntimeException("Exception in fetching state.", e);
      }
    };
  }

  private static Map<TopicPartition, Long> getSavedState(StreamingContext context, ConfluentStreamingSourceConfig conf)
    throws IOException {
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

  @Nonnull
  private static Map<String, Object> getConsumerParams(ConfluentStreamingSourceConfig conf, String pipelineName) {
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, conf.getBrokers());
    // Spark saves the offsets in checkpoints, no need for Kafka to save them
    kafkaParams.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    kafkaParams.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, "500");
    kafkaParams.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    kafkaParams.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    kafkaParams.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required " +
      "username=\"" + conf.getClusterApiKey() + "\" password=\"" + conf.getClusterApiSecret() + "\";");

    if (!Strings.isNullOrEmpty(conf.getSchemaRegistryUrl())) {
      kafkaParams.put("schema.registry.url", conf.getSchemaRegistryUrl());
      kafkaParams.put("basic.auth.credentials.source", "USER_INFO");
      kafkaParams.put("schema.registry.basic.auth.user.info",
                      conf.getSchemaRegistryApiKey() + ":" + conf.getSchemaRegistryApiSecret());
      kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getCanonicalName());
      kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getCanonicalName());
    } else {
      kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
      kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
    }
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // Create a unique string for the group.id using the pipeline name and the topic.
    // group.id is a Kafka consumer property that uniquely identifies the group of
    // consumer processes to which this consumer belongs.
    String groupId = Joiner.on("-")
      .join(pipelineName.length(), conf.getTopic().length(), pipelineName, conf.getTopic());
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    kafkaParams.putAll(conf.getKafkaProperties());
    // change the request timeout to fetch the metadata to be 15 seconds or 1 second greater than session time out ms,
    // since this config has to be greater than the session time out, which is by default 10 seconds
    // the KafkaConsumer at runtime should still use the default timeout 305 seconds or whatever the user provides in
    // kafkaConf
    int requestTimeout =
      Integer.parseInt(conf.getKafkaProperties().getOrDefault(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "20000"));
    if (conf.getKafkaProperties().containsKey(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG)) {
      int sessionTimeout =
        Integer.parseInt(conf.getKafkaProperties().get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) + 1000);
      requestTimeout = Math.max(requestTimeout, sessionTimeout);
    }
    kafkaParams.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
    return kafkaParams;
  }

  @Nonnull
  private static Map<TopicPartition, Long> getOffsets(ConfluentStreamingSourceConfig conf, FailureCollector collector,
                                                      Consumer<byte[], byte[]> consumer,
                                                      Supplier<Map<TopicPartition, Long>> stateSupplier) {

    Map<TopicPartition, Long> offsets = getInitialPartitionOffsets(conf, stateSupplier, consumer, collector);

    // KafkaUtils doesn't understand -1 and -2 as smallest offset and latest offset.
    // so we have to replace them with the actual smallest and latest
    List<TopicPartition> earliestOffsetRequest = new ArrayList<>();
    List<TopicPartition> latestOffsetRequest = new ArrayList<>();
    for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
      TopicPartition topicAndPartition = entry.getKey();
      Long offset = entry.getValue();
      if (offset == ListOffsetRequest.EARLIEST_TIMESTAMP) {
        earliestOffsetRequest.add(topicAndPartition);
      } else if (offset == ListOffsetRequest.LATEST_TIMESTAMP) {
        latestOffsetRequest.add(topicAndPartition);
      }
    }

    Set<TopicPartition> allOffsetRequest =
      Sets.newHashSet(Iterables.concat(earliestOffsetRequest, latestOffsetRequest));
    Map<TopicPartition, Long> offsetsFound = new HashMap<>();
    offsetsFound.putAll(KafkaHelpers.getEarliestOffsets(consumer, earliestOffsetRequest));
    offsetsFound.putAll(KafkaHelpers.getLatestOffsets(consumer, latestOffsetRequest));
    for (TopicPartition topicAndPartition : allOffsetRequest) {
      offsets.put(topicAndPartition, offsetsFound.get(topicAndPartition));
    }

    Set<TopicPartition> missingOffsets = Sets.difference(allOffsetRequest, offsetsFound.keySet());
    if (!missingOffsets.isEmpty()) {
      throw new IllegalStateException(String.format(
        "Could not find offsets for %s. Please check all brokers were included in the broker list.", missingOffsets));
    }
    return offsets;
  }

  static Map<TopicPartition, Long> getInitialPartitionOffsets(ConfluentStreamingSourceConfig conf,
                                                              Supplier<Map<TopicPartition, Long>> stateSupplier,
                                                              Consumer<byte[], byte[]> consumer,
                                                              FailureCollector collector) {
    Map<TopicPartition, Long> savedPartitions = stateSupplier.get();
    if (!savedPartitions.isEmpty()) {
      LOG.info("Saved partitions found {}. ", savedPartitions);
      return savedPartitions;
    }

    LOG.info("No saved partitions found.");
    Map<TopicPartition, Long> offsets = conf.getInitialPartitionOffsets(
      getPartitions(consumer, conf, collector), collector);
    collector.getOrThrowException();
    return offsets;
  }

  private static Set<Integer> getPartitions(Consumer<byte[], byte[]> consumer, ConfluentStreamingSourceConfig conf,
                                            FailureCollector collector) {
    Set<Integer> partitions = conf.getPartitions(collector);
    collector.getOrThrowException();

    if (!partitions.isEmpty()) {
      return partitions;
    }

    partitions = new HashSet<>();
    for (PartitionInfo partitionInfo : consumer.partitionsFor(conf.getTopic())) {
      partitions.add(partitionInfo.partition());
    }
    return partitions;
  }

  private static Function2<ConsumerRecord<Object, Object>, Time, StructuredRecord>
  getTransformFunction(ConfluentStreamingSourceConfig conf, Schema outputSchema) {
    if (conf.getSchemaRegistryUrl() != null) {
      return new AvroFunction(conf, outputSchema);
    } else {
      return conf.getFormat() == null ? new BytesFunction(conf, outputSchema) : new FormatFunction(conf, outputSchema);
    }
  }

  /**
   * Applies the format function to each rdd.
   */
  static class AvroRecordTransform
    implements Function2<JavaRDD<ConsumerRecord<Object, Object>>, Time, JavaRDD<StructuredRecord>> {

    private final ConfluentStreamingSourceConfig conf;
    private final Schema outputSchema;

    AvroRecordTransform(ConfluentStreamingSourceConfig conf, Schema outputSchema) {
      this.conf = conf;
      this.outputSchema = outputSchema;
    }

    @Override
    public JavaRDD<StructuredRecord> call(JavaRDD<ConsumerRecord<Object, Object>> input, Time batchTime) {
      Function2<ConsumerRecord<Object, Object>, Time, StructuredRecord> recordFunction =
        new AvroFunction(conf, outputSchema);

      return input.map((Function<ConsumerRecord<Object, Object>, StructuredRecord>) consumerRecord ->
        recordFunction.call(consumerRecord, batchTime));
    }
  }

  /**
   * Applies the format function to each rdd.
   */
  static class RecordTransform
    implements Function2<JavaRDD<ConsumerRecord<Object, Object>>, Time, JavaRDD<StructuredRecord>> {

    private final ConfluentStreamingSourceConfig conf;
    private final Schema outputSchema;

    RecordTransform(ConfluentStreamingSourceConfig conf, Schema outputSchema) {
      this.conf = conf;
      this.outputSchema = outputSchema;
    }

    @Override
    public JavaRDD<StructuredRecord> call(JavaRDD<ConsumerRecord<Object, Object>> input, Time batchTime) {
      Function2<ConsumerRecord<Object, Object>, Time, StructuredRecord> recordFunction = conf.getFormat() == null ?
        new BytesFunction(conf, outputSchema) :
        new FormatFunction(conf, outputSchema);
      return input.map((Function<ConsumerRecord<Object, Object>, StructuredRecord>) consumerRecord ->
        recordFunction.call(consumerRecord, batchTime));
    }
  }

  /**
   * Common logic for transforming kafka key, message, partition, and offset into a structured record.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private abstract static class BaseFunction<K, V> implements Function2<ConsumerRecord<K, V>, Time, StructuredRecord> {
    protected final ConfluentStreamingSourceConfig conf;
    private final Schema outputSchema;

    BaseFunction(ConfluentStreamingSourceConfig conf, Schema outputSchema) {
      this.conf = conf;
      this.outputSchema = outputSchema;
    }

    @Override
    public StructuredRecord call(ConsumerRecord<K, V> in, Time batchTime) throws Exception {
      String timeField = conf.getTimeField();
      String keyField = conf.getKeyField();
      String partitionField = conf.getPartitionField();
      String offsetField = conf.getOffsetField();
      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
      if (timeField != null) {
        builder.set(timeField, batchTime.milliseconds());
      }
      if (keyField != null) {
        builder.set(keyField, convertKey(in.key()));
      }
      if (partitionField != null) {
        builder.set(partitionField, in.partition());
      }
      if (offsetField != null) {
        builder.set(offsetField, in.offset());
      }
      addMessage(builder, in.value());
      return builder.build();
    }

    protected abstract Object convertKey(K key);

    protected abstract void addMessage(StructuredRecord.Builder builder, V message) throws Exception;
  }

  private abstract static class BinaryBaseFunction extends BaseFunction<Object, Object> {
    BinaryBaseFunction(ConfluentStreamingSourceConfig conf, Schema outputSchema) {
      super(conf, outputSchema);
    }

    @Override
    protected Object convertKey(Object key) {
      if (key == null) {
        return null;
      }
      Schema keySchemaNullable = conf.getSchema().getField(conf.getKeyField()).getSchema();
      Schema keySchema = keySchemaNullable.isNullable() ? keySchemaNullable.getNonNullable() : keySchemaNullable;
      if (keySchema.getType() == Schema.Type.STRING) {
        return new String((byte[]) key, StandardCharsets.UTF_8);
      }
      if (keySchema.getType() == Schema.Type.BYTES) {
        return key;
      }
      throw new IllegalStateException(String.format("Unexpected key type '%s'", keySchema.getDisplayName()));
    }
  }

  /**
   * Transforms kafka key and message into a structured record when message format is not given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private static class BytesFunction extends BinaryBaseFunction {
    private transient String messageField;

    BytesFunction(ConfluentStreamingSourceConfig conf, Schema outputSchema) {
      super(conf, outputSchema);
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, Object message) {
      builder.set(getMessageField(), message);
    }

    private String getMessageField() {
      if (messageField == null) {
        for (Schema.Field field : conf.getSchema().getFields()) {
          String name = field.getName();
          if (!name.equals(conf.getTimeField()) && !name.equals(conf.getKeyField())
            && !name.equals(conf.getOffsetField()) && !name.equals(conf.getPartitionField())) {
            messageField = name;
            break;
          }
        }
        if (messageField == null) {
          throw new IllegalStateException("No message field found in schema");
        }
      }
      return messageField;
    }
  }

  /**
   * Transforms kafka key and message into a structured record when message format and schema are given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private static class FormatFunction extends BinaryBaseFunction {
    private transient RecordFormat<ByteBuffer, StructuredRecord> recordFormat;

    FormatFunction(ConfluentStreamingSourceConfig conf, Schema outputSchema) {
      super(conf, outputSchema);
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, Object message) throws Exception {
      // first time this was called, initialize record format
      if (recordFormat == null) {
        Schema messageSchema = conf.getMessageSchema();
        FormatSpecification spec = new FormatSpecification(conf.getFormat(), messageSchema, new HashMap<>());
        recordFormat = RecordFormats.createInitializedFormat(spec);
      }

      StructuredRecord messageRecord = recordFormat.read(ByteBuffer.wrap((byte[]) message));
      for (Schema.Field field : messageRecord.getSchema().getFields()) {
        String fieldName = field.getName();
        builder.set(fieldName, messageRecord.get(fieldName));
      }
    }
  }

  private static class AvroFunction extends BaseFunction<Object, Object> {
    private transient AvroToStructuredTransformer transformer;

    AvroFunction(ConfluentStreamingSourceConfig conf, Schema outputSchema) {
      super(conf, outputSchema);
    }

    @Override
    protected Object convertKey(Object key) {
      return key;
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, Object message) throws Exception {
      if (transformer == null) {
        transformer = new AvroToStructuredTransformer();
      }
      if (!(message instanceof GenericRecord)) {
        throw new UnexpectedFormatException(
          String.format("Unexpected message class '%s'", message.getClass().getName()));
      }
      GenericRecord genericRecord = (GenericRecord) message;
      StructuredRecord messageRecord = transformer.transform(genericRecord);
      builder.set(conf.getvalueSchema(), messageRecord);
    }
  }
}
