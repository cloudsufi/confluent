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

package io.cdap.plugin.confluent.integration.streaming;

import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.mock.transform.RecoveringTransform;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.confluent.StructuredRecordRepresentation;
import io.cdap.plugin.confluent.streaming.sink.ConfluentStreamingSink;
import io.cdap.plugin.confluent.streaming.source.ConfluentStreamingSource;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class ConfluentStreamingTestBase extends HydratorTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG =
    new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                          Constants.AppFabric.SPARK_COMPAT, Compat.SPARK_COMPAT);
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(ConfluentStreamingTestBase.class);
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-streams", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-streams", "1.0.0");

  @BeforeClass
  public static void setupBasic() throws Exception {
    LOG.info("Setting up application");

    setupStreamingArtifacts(APP_ARTIFACT_ID, DataStreamsApp.class);

    LOG.info("Setting up plugins");
    addPluginArtifact(
      NamespaceId.DEFAULT.artifact("confluent-kafka-plugins", "1.1.0"),
      APP_ARTIFACT_ID,
      ConfluentStreamingSource.class, ConfluentStreamingSink.class,
      KafkaUtils.class, TopicPartition.class,
      ByteArrayDeserializer.class, ByteArraySerializer.class,
      KafkaAvroDeserializer.class, KafkaAvroSerializer.class
    );
  }

  protected SparkManager deployETL(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin, String appName, boolean isRecovery)
    throws Exception {
    ETLStage source = new ETLStage("source", sourcePlugin);
    ETLStage sink = new ETLStage("sink", sinkPlugin);
    
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setBatchInterval("1s")
      .setStopGracefully(true)
      .build();
    if (isRecovery) {
      etlConfig = DataStreamsConfig.builder()
        .addStage(source)
        .addStage(new ETLStage("retry_transform", RecoveringTransform.getPlugin()))
        .addStage(sink)
        .addConnection("source", "retry_transform")
        .addConnection("retry_transform", "sink")
        .build();
    }
    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);
    return getProgramManager(applicationManager);
  }

  protected List<StructuredRecord> waitForRecords(String outputTable, int messageCount) throws Exception {
    DataSetManager<Table> outputManager = getDataset(outputTable);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
      List<StructuredRecord> output = MockSink.readOutput(outputManager);
      Assertions.assertThat(output)
        .withRepresentation(new StructuredRecordRepresentation())
        .hasSizeGreaterThanOrEqualTo(messageCount);
    });

    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assertions.assertThat(output)
      .withRepresentation(new StructuredRecordRepresentation())
      .hasSize(messageCount);
    return output;
  }

  protected void waitForRecords(String outputTable, List<StructuredRecord> expectedRecords) throws Exception {
    DataSetManager<Table> outputManager = getDataset(outputTable);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
      List<StructuredRecord> output = MockSink.readOutput(outputManager);
      Assertions.assertThat(output)
        .withRepresentation(new StructuredRecordRepresentation())
        .hasSizeGreaterThanOrEqualTo(expectedRecords.size());
    });

    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assertions.assertThat(output)
      .withRepresentation(new StructuredRecordRepresentation())
      .containsExactlyInAnyOrderElementsOf(expectedRecords);
  }

  private SparkManager getProgramManager(ApplicationManager appManager) {
    return appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
  }
}
