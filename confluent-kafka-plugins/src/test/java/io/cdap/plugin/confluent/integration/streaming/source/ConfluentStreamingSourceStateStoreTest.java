/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.confluent.integration.streaming.source;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import io.cdap.cdap.api.app.AppStateStore;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestBase;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.batch.source.KafkaPartitionOffsets;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.http.HTTPPollConfig;
import io.cdap.plugin.confluent.integration.KafkaTestUtils;
import io.cdap.plugin.confluent.source.ConfluentDStream;
import io.cdap.plugin.confluent.streaming.source.ConfluentStreamingSource;
import io.cdap.plugin.confluent.streaming.source.ConfluentStreamingSourceConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests for Confluent streaming source with state store
 */
public class ConfluentStreamingSourceStateStoreTest extends HydratorTestBase {

  // Turn on state tracking.
  @ClassRule
  public static final TestConfiguration CONFIG =
    new TestConfiguration("explore.enabled", false,
                          "feature.streaming.pipeline.native.state.tracking.enabled", "true");
  private static final Gson GSON = new Gson();
  private static final ArtifactId DATASTREAMS_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-streams", "6.8.0");
  private static final ArtifactSummary DATASTREAMS_ARTIFACT =
    new ArtifactSummary("data-streams", "6.8.0");
  private static final String SRC_STAGE_NAME = "source";
  private static final String TOPIC_NAME = "users";
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  private static KafkaProducer<byte[], byte[]> kafkaProducer;
  private static KafkaProducer<Object, Object> kafkaAvroProducer;

  @BeforeClass
  public static void setupTest() throws Exception {
    setupStreamingArtifacts(DATASTREAMS_ARTIFACT_ID, DataStreamsApp.class);

    // add artifact for Confluent plugins
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATASTREAMS_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true)
    );
    addPluginArtifact(NamespaceId.DEFAULT.artifact("kafka-plugins", "1.0.0"), parents,
                      ConfluentStreamingSource.class, KafkaUtils.class, Deserializer.class, ByteArrayDeserializer.class,
                      TopicPartition.class, HTTPPollConfig.class, ConfluentDStream.class);
    // Initialize kafka server
    kafkaProducer = KafkaTestUtils.createProducer();
    kafkaAvroProducer = KafkaTestUtils.createProducerForSchemaRegistry();
  }

  @AfterClass
  public static void cleanup() {
    if (kafkaProducer != null) {
      kafkaProducer.close();
    }
    if (kafkaAvroProducer != null) {
      kafkaAvroProducer.close();
    }
  }

  @Test
  public void testConfluentStreamingSource() throws Exception {
    KafkaTestUtils.deleteTopic(TOPIC_NAME);
    KafkaTestUtils.createTopic(TOPIC_NAME, 2, 3);
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)));

    Map<String, String> properties = getConfigProperties(schema);
    properties.put(ConfluentStreamingSourceConfig.NAME_FORMAT, "csv");

    ETLStage source = new ETLStage(SRC_STAGE_NAME, new ETLPlugin(ConfluentStreamingSource.PLUGIN_NAME,
                                                                 StreamingSource.PLUGIN_TYPE, properties, null));

    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(new ETLStage("sink", MockSink.getPlugin("kafkaOutput")))
      .addConnection(SRC_STAGE_NAME, "sink")
      .setBatchInterval("1s")
      .setStopGracefully(true)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(DATASTREAMS_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("KafkaSourceApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write some messages to kafka
    Map<String, String> messages = new HashMap<>();
    messages.put("a", "1,samuel,jackson");
    messages.put("b", "2,dwayne,johnson");
    messages.put("c", "3,christopher,walken");
    for (Map.Entry<String, String> entry : messages.entrySet()) {
      sendKafkaMessage(TOPIC_NAME, 0, entry.getKey(), entry.getValue());
    }

    // Save an entry for offset 1 (second in the data) in state store with reference name.
    AppStateStore appStateStore = TestBase.getAppStateStore(appId.getNamespace(), appId.getApplication());
    appStateStore.saveState(SRC_STAGE_NAME + "." + TOPIC_NAME,
                            GSON.toJson(new KafkaPartitionOffsets(Collections.singletonMap(0, 1L)))
                              .getBytes(StandardCharsets.UTF_8));

    // Launch the program and wait for results
    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 2, TimeUnit.MINUTES);

    final DataSetManager<Table> outputManager = getDataset("kafkaOutput");
    Tasks.waitFor(
      ImmutableMap.of(2L, "dwayne johnson", 3L, "christopher walken"),
      () -> {
        outputManager.flush();
        Map<Long, String> actual = new HashMap<>();
        for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
          actual.put(outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
        }
        return actual;
      }, 2, TimeUnit.MINUTES);

    // Verify that state is saved with the next offset to start from.
    Tasks.waitFor(3L, () -> {
      Optional<byte[]> savedState = appStateStore.getState(SRC_STAGE_NAME + "." + TOPIC_NAME);
      try (Reader reader = new InputStreamReader(new ByteArrayInputStream(savedState.get()),
                                                 StandardCharsets.UTF_8)) {
        KafkaPartitionOffsets partitionOffsets = GSON.fromJson(reader, KafkaPartitionOffsets.class);
        Long savedOffset = partitionOffsets.getPartitionOffsets().get(0);
        return savedOffset.longValue();
      }
    }, 2, TimeUnit.MINUTES);

    // stop the run
    sparkManager.stop();
    sparkManager.waitForRun(ProgramRunStatus.KILLED, 2, TimeUnit.MINUTES);
  }

  private Map<String, String> getConfigProperties(Schema schema) {
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.Reference.REFERENCE_NAME, "confluent");
    properties.put(ConfluentStreamingSourceConfig.NAME_BROKERS, KafkaTestUtils.KAFKA_SERVER);
    properties.put(ConfluentStreamingSourceConfig.NAME_TOPIC, TOPIC_NAME);
    properties.put(ConfluentStreamingSourceConfig.NAME_DEFAULT_INITIAL_OFFSET,
                   String.valueOf(ListOffsetsRequest.EARLIEST_TIMESTAMP));
    properties.put(ConfluentStreamingSourceConfig.NAME_CLUSTER_API_KEY, KafkaTestUtils.CLUSTER_API_KEY);
    properties.put(ConfluentStreamingSourceConfig.NAME_CLUSTER_API_SECRET, KafkaTestUtils.CLUSTER_API_SECRET);
    properties.put(ConfluentStreamingSourceConfig.NAME_SCHEMA, schema.toString());
    properties.put(ConfluentStreamingSourceConfig.NAME_MAX_RATE, "1000");
    return properties;
  }

  private void sendKafkaMessage(String topic, @Nullable Integer partition, @Nullable String key, String value) {
    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
    byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
    try {
      kafkaProducer.send(new ProducerRecord<>(topic, partition, keyBytes, valueBytes)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }
}
