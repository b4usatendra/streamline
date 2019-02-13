/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 **/
package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.common.exception.ComponentConfigException;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.common.event.sedes.kafka.KafkaDeserializer;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.beam.rule.expression.BeamUtilFunctions;
import com.hortonworks.streamline.streams.layout.component.StreamlineSource;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import joptsimple.internal.Strings;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Implementation for KafkaSpout
 */
public class BeamKafkaSourceComponent extends AbstractBeamComponent {

  static final String SASL_JAAS_CONFIG_KEY = "saslJaasConfig";
  static final String SASL_KERBEROS_SERVICE_NAME = "kafkaServiceName";
  private static final Logger LOG = LoggerFactory.getLogger(BeamKafkaSourceComponent.class);
  private StreamlineSource kafkaSource;

  // for unit tests
  public BeamKafkaSourceComponent() {
  }

  @Override
  public PCollection getOutputCollection() {
    return outputCollection;
  }

  @Override
  public void validateConfig() throws ComponentConfigException {

  }

  @Override
  public void unionInputCollection(PCollection collection) {
    throw new NotImplementedException();
  }

  @Override
  public void generateComponent(PCollection inputCollection) {
    kafkaSource = (StreamlineSource) conf.get(TopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);
    if (!isGenerated) {
      StreamlineSource streamlineSource = (StreamlineSource) conf
          .get(TopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);

      String outputStream = (String) conf.get(TopologyLayoutConstants.JSON_KEY_OUTPUT_STREAM_ID);
      String sourceId = streamlineSource.getId();

      initializeComponent();
      isGenerated = true;
    }
  }

  //TODO create constants class for all kafka related constants
  private void initializeComponent() {

    String beamSourceId = "beamKafkaSource" + UUID_FOR_COMPONENTS;
    KafkaIO.Read<Object, StreamlineEvent> reader = null;

    //TODO remove this section
    if (!conf.containsKey(TopologyLayoutConstants.JSON_KEY_KEY_DESERIALIZATION)) {
      conf.put(TopologyLayoutConstants.JSON_KEY_KEY_DESERIALIZATION, KafkaDeserializer.StringDeserializer.name());
    }

    if (!conf.containsKey(TopologyLayoutConstants.JSON_KEY_VALUE_DESERIALIZATION)) {
      conf.put(TopologyLayoutConstants.JSON_KEY_VALUE_DESERIALIZATION, KafkaDeserializer.FabricEventJsonDeserializer.name());
    }

    if (!conf.containsKey(BeamTopologyLayoutConstants.KAFKA_CLIENT_USER)) {
      conf.put(BeamTopologyLayoutConstants.KAFKA_CLIENT_USER, "olauser");
    }

    if (!conf.containsKey(BeamTopologyLayoutConstants.KAFKA_CLIENT_PASSWORD)) {
      conf.put(BeamTopologyLayoutConstants.KAFKA_CLIENT_PASSWORD, "olauser@1234");
    }

    if (!conf.containsKey(BeamTopologyLayoutConstants.ZK_CLIENT_USER)) {
      conf.put(BeamTopologyLayoutConstants.ZK_CLIENT_USER, "zkclient");
    }

    if (!conf.containsKey(BeamTopologyLayoutConstants.ZK_CLIENT_PASSWORD)) {
      conf.put(BeamTopologyLayoutConstants.ZK_CLIENT_PASSWORD, "Ukidfds#59");
    }

    String valueDeserializer = (String) conf.get(TopologyLayoutConstants.JSON_KEY_VALUE_DESERIALIZATION);
    String keyDeserializer = (String) conf.get(TopologyLayoutConstants.JSON_KEY_KEY_DESERIALIZATION);
    String bootstrapServers = (String) conf.get(TopologyLayoutConstants.JSON_KEY_BOOTSTRAP_SERVER);
    String topic = (String) conf.get(TopologyLayoutConstants.JSON_KEY_TOPIC);

    try {

      reader = KafkaIO.<Object, ByteBuffer>read()
          .withBootstrapServers(bootstrapServers)
          .withTopics(Arrays.asList((topic)))
          .withKeyDeserializer(KafkaDeserializer.getDeserializer(keyDeserializer))
          .withValueDeserializer(KafkaDeserializer.getDeserializer(valueDeserializer))
          .updateConsumerProperties(addConsumerProperties());

      outputCollection = this.pipeline
          .apply(beamSourceId, reader.withoutMetadata())
          .apply("beamKafkaInputParDo", BeamUtilFunctions.extractStreamlineEvents(beamSourceId));
    } catch (Exception e) {
      throw new RuntimeException("Class not found: " + e.getMessage());
    }


  }

  private Map<String, Object> addConsumerProperties() {
    Map<String, Object> consumerProperties = new HashMap<String, Object>();

    String[] propertyNames = {
        "group.id", "fetch.min.bytes", "max.partition.fetch.bytes", "max.poll.records",
        "security.protocol", "schema.registry.url",
        "reader.schema.version"

    };
    String[] fieldNames = {
        "consumerGroupId", "fetchMinimumBytes", "fetchMaximumBytesPerPartition",
        "maxRecordsPerPoll", "securityProtocol", "schemaRegistryUrl"
        , "readerSchemaVersion"
    };

    for (int j = 0; j < propertyNames.length; ++j) {
      if (conf.get(fieldNames[j]) != null) {
        consumerProperties.put(propertyNames[j], conf.get(fieldNames[j]));
      }
    }
    setSaslJaasConfig(consumerProperties);
    return consumerProperties;
  }

  private void setSaslJaasConfig(Map<String, Object> consumerProperties) {
    String securityProtocol = (String) conf.get("securityProtocol");
    String jaasConfigStr = null;
    String jaasFilePath = BeamKafkaSourceComponent.class.getClassLoader().getResource("flink-jaas.conf").getPath();

    if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol
        .equals("SASL_PLAINTEXT")) {
      consumerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      consumerProperties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

    }

    if (!Strings.isNullOrEmpty(jaasFilePath)) {
      System.out.println(jaasFilePath);
      File consumerJaasFile = new File(jaasFilePath);
      //System.setProperty("java.security.auth.login.config", "/tmp/topology/Beam_Aggregator_Test/jaas/jaas.conf");
      System.out.println(System.getProperty("java.security.auth.login.config"));
    }
  }


}
