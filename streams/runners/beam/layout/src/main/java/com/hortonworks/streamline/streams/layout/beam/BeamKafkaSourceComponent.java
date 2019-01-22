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
      conf.put(TopologyLayoutConstants.JSON_KEY_KEY_DESERIALIZATION, "ByteArrayDeserializer");
    }

    if (!conf.containsKey(TopologyLayoutConstants.JSON_KEY_VALUE_DESERIALIZATION)) {
      conf.put(TopologyLayoutConstants.JSON_KEY_VALUE_DESERIALIZATION, "StreamlineAvroDeserialzer");
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
      throw new RuntimeException("Class not found");
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

    validateSSLConfig();
    setSaslJaasConfig(consumerProperties);
    return consumerProperties;
  }

  private void setSaslJaasConfig(Map<String, Object> consumerProperties) {
    String securityProtocol = (String) conf.get("securityProtocol");
    String jaasConfigStr = null;
    if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol
        .equals("SASL_PLAINTEXT")) {
      consumerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      consumerProperties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

      jaasConfigStr = getSaslJaasConfig();

    } else if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol
        .startsWith("SASL")) {
      StringBuilder saslConfigStrBuilder = new StringBuilder();
      String kafkaServiceName = (String) conf.get(SASL_KERBEROS_SERVICE_NAME);
      String principal = (String) conf.get("principal");
      String keytab = (String) conf.get("keytab");
      if (kafkaServiceName == null || kafkaServiceName.isEmpty()) {
        throw new IllegalArgumentException("Kafka service name must be provided for SASL GSSAPI Kerberos");
      }
      if (principal == null || principal.isEmpty()) {
        throw new IllegalArgumentException("Kafka client principal must be provided for SASL GSSAPI Kerberos");
      }
      if (keytab == null || keytab.isEmpty()) {
        throw new IllegalArgumentException("Kafka client principal keytab must be provided for SASL GSSAPI Kerberos");
      }
      saslConfigStrBuilder.append("com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"");
      saslConfigStrBuilder.append(keytab).append("\"  principal=\"").append(principal).append("\";");

      jaasConfigStr = saslConfigStrBuilder.toString();
    }

    if (!Strings.isNullOrEmpty(jaasConfigStr)) {
      String jaasFilePath = (String) conf.get(BeamTopologyLayoutConstants.JAAS_CONF_PATH);
      File consumerJaasFile = new File(jaasFilePath);

      try (Writer writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(jaasFilePath), "utf-8"))) {
        writer.write(jaasConfigStr);
        consumerJaasFile.deleteOnExit();
      } catch (IOException e) {
        throw new RuntimeException("Unable to set security protocol for kafka source");
      }
      System.setProperty("java.security.auth.login.config", consumerJaasFile.getAbsolutePath());
    }
  }

  public String getSaslJaasConfig() {

    String kafkaClientUser = (String) conf.get(BeamTopologyLayoutConstants.KAFKA_CLIENT_USER);
    String kafkaClientPassword = (String) conf.get(BeamTopologyLayoutConstants.KAFKA_CLIENT_PASSWORD);
    String zkClientUser = (String) conf.get(BeamTopologyLayoutConstants.ZK_CLIENT_USER);
    String zkClientPassword = (String) conf.get(BeamTopologyLayoutConstants.ZK_CLIENT_PASSWORD);

    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("KafkaClient {\n")
        .append("org.apache.kafka.common.security.plain.PlainLoginModule required")
        .append("\n")
        .append("username=\"").append(kafkaClientUser).append("\"")
        .append("\n")
        .append("password=\"").append(kafkaClientPassword).append("\"")
        .append(";\n")
        .append("};\n\n");

    stringBuilder.append("Client {\n")
        .append("org.apache.zookeeper.server.auth.DigestLoginModule required")
        .append("\n")
        .append("username=\"").append(zkClientUser).append("\"")
        .append("\n")
        .append("password=\"").append(zkClientPassword).append("\"")
        .append(";\n")
        .append("};\n\n");

    return stringBuilder.toString();

  }

  private void validateSSLConfig() {
    String securityProtocol = (String) conf.get("securityProtocol");
    if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol.endsWith("SSL")) {
      String truststoreLocation = (String) conf.get("sslTruststoreLocation");
      String truststorePassword = (String) conf.get("sslTruststorePassword");
      if (truststoreLocation == null || truststoreLocation.isEmpty()) {
        throw new IllegalArgumentException("Truststore location must be provided for SSL");
      }
      if (truststorePassword == null || truststorePassword.isEmpty()) {
        throw new IllegalArgumentException("Truststore password must be provided for SSL");
      }
    } else if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol
        .equals("SASL_PLAINTEXT")) {

      String userName = (String) conf.get(BeamTopologyLayoutConstants.KAFKA_CLIENT_USER);
      String password = (String) conf.get(BeamTopologyLayoutConstants.KAFKA_CLIENT_PASSWORD);
      String jaasFilePath = (String) conf.get(BeamTopologyLayoutConstants.JAAS_CONF_PATH);

      if (!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password) && !Strings.isNullOrEmpty(jaasFilePath)) {
        return;
      } else {
        throw new RuntimeException("either kafka.user or password is null");
      }
    }
  }
}
