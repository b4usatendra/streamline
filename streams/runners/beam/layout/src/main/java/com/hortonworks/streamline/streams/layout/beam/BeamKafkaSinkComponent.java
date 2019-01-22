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

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.common.event.sedes.kafka.KafkaSerializer;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.beam.rule.expression.BeamUtilFunctions;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
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
 * Implementation for Beam Kafka Producer
 */
public class BeamKafkaSinkComponent extends AbstractBeamComponent {

  static final String SASL_JAAS_CONFIG_KEY = "saslJaasConfig";
  static final String SASL_KERBEROS_SERVICE_NAME = "kafkaServiceName";
  private static final Logger LOG = LoggerFactory.getLogger(BeamKafkaSinkComponent.class);


  public BeamKafkaSinkComponent() {
  }

  @Override
  public PCollection<StreamlineEvent> getOutputCollection() {
    throw new NotImplementedException();
  }

  //TODO add config validation logic
  public void validateConfig() {

  }

  @Override
  public void generateComponent(PCollection pCollection) {
    initializeComponent(pCollection);
  }

  private void initializeComponent(PCollection<StreamlineEvent> pCollection) {
    String sinkId = "beamKafkaSink" + UUID_FOR_COMPONENTS;
    LOG.info("Generating BeamKafkaSinkComponent with id: ", sinkId);
    String routingKey = (String) conf.get("routingKey");

    //TODO remove this section
    if (!conf.containsKey(TopologyLayoutConstants.JSON_KEY_KEY_SERIALIZATION)) {
      conf.put(TopologyLayoutConstants.JSON_KEY_KEY_SERIALIZATION, "ByteArraySerializer");
    }

    if (!conf.containsKey(TopologyLayoutConstants.JSON_KEY_VALUE_SERIALIZATION)) {
      conf.put(TopologyLayoutConstants.JSON_KEY_VALUE_SERIALIZATION, "StreamlineAvroSerialzer");
    }


    if(!conf.containsKey(BeamTopologyLayoutConstants.KAFKA_CLIENT_USER)){
      conf.put(BeamTopologyLayoutConstants.KAFKA_CLIENT_USER, "olauser");
    }

    if(!conf.containsKey(BeamTopologyLayoutConstants.KAFKA_CLIENT_PASSWORD)){
      conf.put(BeamTopologyLayoutConstants.KAFKA_CLIENT_PASSWORD, "olauser@1234");
    }


    if (!conf.containsKey(BeamTopologyLayoutConstants.ZK_CLIENT_USER)) {
      conf.put(BeamTopologyLayoutConstants.ZK_CLIENT_USER, "zkclient");
    }

    if (!conf.containsKey(BeamTopologyLayoutConstants.ZK_CLIENT_PASSWORD)) {
      conf.put(BeamTopologyLayoutConstants.ZK_CLIENT_PASSWORD, "Ukidfds#59");
    }

    String bootstrapServers = (String) conf.get(TopologyLayoutConstants.JSON_KEY_BOOTSTRAP_SERVER);
    String topic = (String) conf.get(TopologyLayoutConstants.JSON_KEY_TOPIC);
    String keySerializer = (String) conf.get(TopologyLayoutConstants.JSON_KEY_KEY_SERIALIZATION);
    String valueSerializer = (String) conf
        .get(TopologyLayoutConstants.JSON_KEY_VALUE_SERIALIZATION);

    KafkaIO.Write<byte[], StreamlineEvent> writer = KafkaIO.<byte[], StreamlineEvent>write()
        .withBootstrapServers(bootstrapServers)
        .withTopic(topic)
        .withKeySerializer(KafkaSerializer.getSerializer(keySerializer))
        .withValueSerializer(KafkaSerializer.getSerializer(valueSerializer))
        .updateProducerProperties(addProducerProperties());

    pCollection.apply("recordKeyGeneration", BeamUtilFunctions.generateKey(sinkId, routingKey))
        .apply(writer);

    if (outputCollection == null) {
      outputCollection = pCollection;
    } else {
      unionInputCollection(pCollection);
    }
  }

  private Map<String, Object> addProducerProperties() {
    String producerPropertiesComponentId = "producerProperties" + UUID_FOR_COMPONENTS;

    Map<String, Object> producerProperties = new HashMap<>();

    //fieldNames and propertyNames arrays should be of same length
    String[] propertyNames = {
        "bootstrap.servers", "buffer.memory", "compression.type", "retries", "batch.size",
        "client.id", "connections.max.idle.ms",
        "linger.ms", "max.block.ms", "max.request.size", "receive.buffer.bytes",
        "request.timeout.ms", "security.protocol", "send.buffer.bytes",
        "timeout.ms", "block.on.buffer.full", "max.in.flight.requests.per.connection",
        "metadata.fetch.timeout.ms", "metadata.max.age.ms",
        "reconnect.backoff.ms", "retry.backoff.ms", "acks",
        "schema.registry.url", "serdes.protocol.version", "writer.schema.version"
    };
    String[] fieldNames = {
        "bootstrapServers", "bufferMemory", "compressionType", "retries", "batchSize", "clientId",
        "maxConnectionIdle",
        "lingerTime", "maxBlock", "maxRequestSize", "receiveBufferSize", "requestTimeout",
        "securityProtocol", "sendBufferSize",
        "timeout", "blocKOnBufferFull", "maxInflighRequests", "metadataFetchTimeout",
        "metadataMaxAge", "reconnectBackoff", "retryBackoff", getAckMode(),
        TopologyLayoutConstants.SCHEMA_REGISTRY_URL, "serProtocolVersion", "writerSchemaVersion"
    };

    for (int j = 0; j < propertyNames.length; ++j) {
      if (conf.get(fieldNames[j]) != null) {
        producerProperties.put(propertyNames[j], conf.get(fieldNames[j]));
      }
    }
    validateSSLConfig();
    setSaslJaasConfig(producerProperties);
    return producerProperties;
  }


  private String getAckMode() {
    String ackMode = (String) conf.get("ackMode");
    if ("None".equals(ackMode)) {
      return "0";
    } else if ("Leader".equals(ackMode) || (ackMode == null)) {
      return "1";
    } else if ("All".equals(ackMode)) {
      return "all";
    } else {
      throw new IllegalArgumentException("Ack mode for kafka sink is not supported: " + ackMode);
    }
  }

  private void setSaslJaasConfig(Map<String, Object> producerProperties) {
    String securityProtocol = (String) conf.get("securityProtocol");
    String jaasConfigStr = null;

    if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol
        .equals("SASL_PLAINTEXT")) {

      producerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      producerProperties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

      jaasConfigStr = getSaslJaasConfig();

    } else if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol
        .equals("SASL_SSL")) {

      StringBuilder saslConfigStrBuilder = new StringBuilder();
      String kafkaServiceName = (String) conf.get(SASL_KERBEROS_SERVICE_NAME);
      String principal = (String) conf.get("principal");
      String keytab = (String) conf.get("keytab");
      if (kafkaServiceName == null || kafkaServiceName.isEmpty()) {
        throw new IllegalArgumentException(
            "Kafka service name must be provided for SASL GSSAPI Kerberos");
      }
      if (principal == null || principal.isEmpty()) {
        throw new IllegalArgumentException(
            "Kafka client principal must be provided for SASL GSSAPI Kerberos");
      }
      if (keytab == null || keytab.isEmpty()) {
        throw new IllegalArgumentException(
            "Kafka client principal keytab must be provided for SASL GSSAPI Kerberos");
      }
      saslConfigStrBuilder.append(
          "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"");
      saslConfigStrBuilder.append(keytab).append("\"  principal=\"").append(principal)
          .append("\";");
      jaasConfigStr = saslConfigStrBuilder.toString();
    }

    if (!Strings.isNullOrEmpty(jaasConfigStr)) {
      String jaasFilePath = (String) conf.get(BeamTopologyLayoutConstants.JAAS_CONF_PATH);
      File producerJaasFile = new File(jaasFilePath);

      try (Writer writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(jaasFilePath), "utf-8"))) {
        writer.write(jaasConfigStr);
        producerJaasFile.deleteOnExit();
      } catch (IOException e) {
        throw new RuntimeException("Unable to set security protocol for kafka source");
      }
      System.setProperty("java.security.auth.login.config", producerJaasFile.getAbsolutePath());
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
    if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol
        .endsWith("SSL")) {
      String truststoreLocation = (String) conf.get("sslTruststoreLocation");
      String truststorePassword = (String) conf.get("sslTruststorePassword");
      if (truststoreLocation == null || truststoreLocation.isEmpty()) {
        throw new IllegalArgumentException("Truststore location must be provided for SSL");
      }
      if (truststorePassword == null || truststorePassword.isEmpty()) {
        throw new IllegalArgumentException("Truststore password must be provided for SSL");
      }
    } else if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol
        .endsWith("SASL")) {

      String userName = (String) conf.get(BeamTopologyLayoutConstants.KAFKA_CLIENT_USER);
      String password = (String) conf.get(BeamTopologyLayoutConstants.KAFKA_CLIENT_PASSWORD);
      String jaasFilePath = (String) conf.get(BeamTopologyLayoutConstants.JAAS_CONF_PATH);

      if (!Strings.isNullOrEmpty(userName) && Strings.isNullOrEmpty(password) && !Strings.isNullOrEmpty(jaasFilePath)) {
        return;
      }
    }
  }
}
