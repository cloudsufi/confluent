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

package io.cdap.plugin.confluent.integration.streaming.source;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.app.AppStateStore;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.transform.RecoveringTransform;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestBase;
import io.cdap.plugin.batch.source.KafkaPartitionOffsets;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.confluent.integration.KafkaTestUtils;
import io.cdap.plugin.confluent.integration.streaming.ConfluentStreamingTestBase;
import io.cdap.plugin.confluent.streaming.source.ConfluentStreamingSource;
import io.cdap.plugin.confluent.streaming.source.ConfluentStreamingSourceConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests for Confluent Streaming Source plugin.
 */
public class ConfluentStreamingSourceStateStoreRecoveryTest extends ConfluentStreamingTestBase {

  private static KafkaProducer<byte[], byte[]> kafkaProducer;
  private static KafkaProducer<Object, Object> kafkaAvroProducer;
  private static final Gson GSON = new Gson();

  @Rule
  public TestName testName = new TestName();

  private String topic;
  private String outputTable;
  private SparkManager programManager;

  @BeforeClass
  public static void setupTestClass() {
    kafkaProducer = KafkaTestUtils.createProducer();
    kafkaAvroProducer = KafkaTestUtils.createProducerForSchemaRegistry();
  }

  @AfterClass
  public static void cleanupTestClass() {
    RecoveringTransform.reset();
    kafkaProducer.close();
    kafkaAvroProducer.close();
  }

  @Before
  public void setUp() {
    outputTable = testName.getMethodName() + "_out";
    topic = ConfluentStreamingSourceStateStoreRecoveryTest.class.getSimpleName() + "_" + testName.getMethodName();
    KafkaTestUtils.deleteTopic(topic);
    KafkaTestUtils.createTopic(topic, 2, 3);
  }

  @After
  public void tearDown() throws Exception {
    KafkaTestUtils.deleteTopic(topic);
    if (programManager != null) {
      programManager.stop();
      programManager.waitForStopped(10, TimeUnit.SECONDS);
      programManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testConfluentStreamingSource() throws Exception {
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING))
    );
    Map<String, String> properties = getConfigProperties(schema);
    properties.put(ConfluentStreamingSourceConfig.NAME_FORMAT, "csv");
    programManager = deploySourcePlugin(properties);

    ApplicationId appId = NamespaceId.DEFAULT.app("KafkaSourceApp");
    // Save an entry for offset 1 (second in the data) in state store with reference name.
    AppStateStore appStateStore = TestBase.getAppStateStore(appId.getNamespace(), appId.getApplication());
    appStateStore.saveState("source" + "." + topic,
                            GSON.toJson(new KafkaPartitionOffsets(Collections.singletonMap(0, 1L)))
                              .getBytes(StandardCharsets.UTF_8));
    
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 2, TimeUnit.MINUTES);

    // write some messages to kafka
    Map<String, String> messages = new HashMap<>();
    messages.put("a", "1,samuel,jackson");
    messages.put("b", "2,dwayne,johnson");
    messages.put("c", "3,christopher,walken");
    for (Map.Entry<String, String> entry : messages.entrySet()) {
      sendKafkaMessage(topic, 0, entry.getKey(), entry.getValue());
    }

    final DataSetManager<Table> outputManager = getDataset("kafkaOutput");
    // Should start reading from offset 1, so should skip the message at offset 0.
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
      Optional<byte[]> savedState = appStateStore.getState("source" + "." + topic);
      try (Reader reader = new InputStreamReader(new ByteArrayInputStream(savedState.get()),
                                                 StandardCharsets.UTF_8)) {
        KafkaPartitionOffsets partitionOffsets = GSON.fromJson(reader, KafkaPartitionOffsets.class);
        Long savedOffset = partitionOffsets.getPartitionOffsets().get(0);
        return savedOffset.longValue();
      }
    }, 2, TimeUnit.MINUTES);


    // stop the run
    programManager.stop();
    programManager.waitForRun(ProgramRunStatus.KILLED, 2, TimeUnit.MINUTES);
  }

  private Map<String, String> getConfigProperties(Schema schema) {
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.Reference.REFERENCE_NAME, "confluent");
    properties.put(ConfluentStreamingSourceConfig.NAME_BROKERS, KafkaTestUtils.KAFKA_SERVER);
    properties.put(ConfluentStreamingSourceConfig.NAME_TOPIC, topic);
    properties.put(ConfluentStreamingSourceConfig.NAME_DEFAULT_INITIAL_OFFSET,
                   String.valueOf(ListOffsetRequest.EARLIEST_TIMESTAMP));
    properties.put(ConfluentStreamingSourceConfig.NAME_CLUSTER_API_KEY, KafkaTestUtils.CLUSTER_API_KEY);
    properties.put(ConfluentStreamingSourceConfig.NAME_CLUSTER_API_SECRET, KafkaTestUtils.CLUSTER_API_SECRET);
    properties.put(ConfluentStreamingSourceConfig.NAME_SCHEMA, schema.toString());
    properties.put(ConfluentStreamingSourceConfig.NAME_MAX_RATE, "1000");
    return properties;
  }

  private SparkManager deploySourcePlugin(Map<String, String> properties) throws Exception {
    return deployETL(
      new ETLPlugin(ConfluentStreamingSource.PLUGIN_NAME, StreamingSource.PLUGIN_TYPE, properties, null),
      MockSink.getPlugin(outputTable),
      "KafkaSourceApp", true
    );
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
