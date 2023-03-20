/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.confluent.streaming.source.ConfluentStreamingSourceConfig;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ConfluentStreamingSourceConfig}.
 */
public class ConfluentStreamingSourceConfigTest {

  @Test
  public void testValidate() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ConfluentStreamingSourceConfig config = ConfluentStreamingSourceConfigHelper.newConfigBuilder()
      .setReferenceName(ConfluentStreamingSourceConfigHelper.TEST_REF_NAME)
      .setBrokers("hostname:9092")
      .setTopic("topic-1")
      .setPartitions("1")
      .setMaxRatePerPartition(1000)
      .setFormat("text")
      .setClusterApiKey(ConfluentStreamingSourceConfigHelper.TEST_CLUSTER_API_KEY)
      .setClusterApiSecret(ConfluentStreamingSourceConfigHelper.TEST_CLUSTER_API_SECRET)
      .build();
    config.validate(mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    assertEquals(0, validationFailures.size());
  }

  @Test
  public void testValidateWhenClusterAPIKeyIsNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ConfluentStreamingSourceConfig config = ConfluentStreamingSourceConfigHelper.newConfigBuilder()
      .setReferenceName(ConfluentStreamingSourceConfigHelper.TEST_REF_NAME)
      .setBrokers("hostname:9092")
      .setTopic("topic-1")
      .setPartitions("1")
      .setMaxRatePerPartition(1000)
      .setFormat("text")
      .setClusterApiKey(null)
      .setClusterApiSecret(ConfluentStreamingSourceConfigHelper.TEST_CLUSTER_API_SECRET)
      .build();
    config.validate(mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    assertEquals(1, validationFailures.size());
    ValidationFailure getResult = validationFailures.get(0);
    assertEquals("Cluster API Key should be provided when Cluster API Secret is used.",
                 getResult.getMessage());
  }
  
  @Test
  public void testValidateWhenClusterAPISecretIsNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ConfluentStreamingSourceConfig config = ConfluentStreamingSourceConfigHelper.newConfigBuilder()
      .setReferenceName(ConfluentStreamingSourceConfigHelper.TEST_REF_NAME)
      .setBrokers("hostname:9092")
      .setTopic("topic-1")
      .setPartitions("1")
      .setMaxRatePerPartition(1000)
      .setFormat("text")
      .setClusterApiKey(ConfluentStreamingSourceConfigHelper.TEST_CLUSTER_API_KEY)
      .setClusterApiSecret(null)
      .build();
    config.validate(mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    assertEquals(1, validationFailures.size());
    ValidationFailure getResult = validationFailures.get(0);
    assertEquals("Cluster API Secret should be provided when Cluster API Key is used.",
                 getResult.getMessage());
  }

  @Test
  public void testValidateWhenBothClusterAPIKeyAndClusterAPISecretAreNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ConfluentStreamingSourceConfig config = ConfluentStreamingSourceConfigHelper.newConfigBuilder()
      .setReferenceName(ConfluentStreamingSourceConfigHelper.TEST_REF_NAME)
      .setBrokers("hostname:9092")
      .setTopic("topic-1")
      .setPartitions("1")
      .setMaxRatePerPartition(1000)
      .setFormat("text")
      .setClusterApiKey(null)
      .setClusterApiSecret(null)
      .build();
    config.validate(mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    assertEquals(0, validationFailures.size());
  }
}
