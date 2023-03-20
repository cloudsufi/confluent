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

import io.cdap.plugin.confluent.streaming.source.ConfluentStreamingSourceConfig;

/**
 * Utility class that provides handy methods to construct Confluent Streaming Source Config for testing
 */
public class ConfluentStreamingSourceConfigHelper {

  public static final String TEST_REF_NAME = "ref";
  public static final String TEST_CLUSTER_API_KEY = "key";
  public static final String TEST_CLUSTER_API_SECRET = "secret";

  public static ConfigBuilder newConfigBuilder() {
    return new ConfigBuilder();
  }

  public static class ConfigBuilder {
    private final String schema = "{" +
      "\"type\":\"record\"," +
      "\"name\":\"etlSchemaBody\"," +
      "\"fields\":[{" +
      "\"name\":\"message\"," +
      "\"type\":\"string\"}]" +
      "}";
    private String referenceName = TEST_REF_NAME;
    private String brokers = "hostname:9092";
    private String topic = "topic-1";
    private String partitions = "1";
    private Integer maxRatePerPartition = 1000;
    private String format = "text";
    private String clusterApiKey = TEST_CLUSTER_API_KEY;
    private String clusterApiSecret = TEST_CLUSTER_API_SECRET;

    public ConfigBuilder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public ConfigBuilder setBrokers(String brokers) {
      this.brokers = brokers;
      return this;
    }

    public ConfigBuilder setTopic(String topic) {
      this.topic = topic;
      return this;
    }

    public ConfigBuilder setPartitions(String partitions) {
      this.partitions = partitions;
      return this;
    }

    public ConfigBuilder setMaxRatePerPartition(Integer maxRatePerPartition) {
      this.maxRatePerPartition = maxRatePerPartition;
      return this;
    }

    public ConfigBuilder setFormat(String format) {
      this.format = format;
      return this;
    }

    public ConfigBuilder setClusterApiKey(String clusterApiKey) {
      this.clusterApiKey = clusterApiKey;
      return this;
    }

    public ConfigBuilder setClusterApiSecret(String clusterApiSecret) {
      this.clusterApiSecret = clusterApiSecret;
      return this;
    }

    public ConfluentStreamingSourceConfig build() {
      return new ConfluentStreamingSourceConfig(referenceName, brokers, topic, partitions, null,
        null, schema, format, null, null, null, null,
        maxRatePerPartition, null, clusterApiKey, clusterApiSecret, null,
        null, null, null);
    }
  }
}
