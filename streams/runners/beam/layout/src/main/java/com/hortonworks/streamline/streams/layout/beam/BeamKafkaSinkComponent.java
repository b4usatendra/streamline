/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/
package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.beam.rule.expression.BeamUtilFunctions;
import java.util.HashMap;
import java.util.Map;
import javassist.bytecode.ByteArray;
import joptsimple.internal.Strings;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.kafka.clients.CommonClientConfigs;
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
  protected PCollection<StreamlineEvent> outputCollection;
  private KafkaSinkComponent kafkaSinkComponent;

  public BeamKafkaSinkComponent() {
  }

  @Override
  public PCollection<StreamlineEvent> getOutputCollection() {
    throw new NotImplementedException();
  }

  @Override
  public void unionInputCollection(PCollection<StreamlineEvent> collection) {
    outputCollection = PCollectionList.of(outputCollection).and(collection)
        .apply(Flatten.<StreamlineEvent>pCollections());
  }

  //TODO add config validation logic
  public void validateConfig() {

  }

  @Override
  public void generateComponent(PCollection pCollection) {
    validateSSLConfig();
    setSaslJaasConfig();
    initializeComponent(pCollection);
  }

  private void initializeComponent(PCollection<StreamlineEvent> pCollection) {
    String sinkId = "beamKafkaSink" + UUID_FOR_COMPONENTS;
    LOG.info("Generating BeamKafkaSinkComponent with id: ", sinkId);
    String routingKey = (String) conf.get("routingKey");

    kafkaSinkComponent = getKafkaSinkComponent();
    KafkaIO.Write<byte[], StreamlineEvent> writer = kafkaSinkComponent
        .getKafkaSink(conf, (String) conf.get("bootstrapServers"), (String) conf.get("topic"),
            addProducerProperties());
    pCollection.apply("recordKeyGeneration", BeamUtilFunctions.generateKey(sinkId, routingKey))
        .apply(writer);

    if (outputCollection == null) {
      outputCollection = pCollection;
    } else {
      unionInputCollection(pCollection);
    }
  }

  private KafkaSinkComponent getKafkaSinkComponent() {
    String keySerializer = (String) conf.get("keySerializer");
    KafkaSinkComponent kafkaSinkComponent = null;

    if ((keySerializer == null) || "ByteArray".equals(keySerializer)) {
      kafkaSinkComponent = new KafkaSinkComponent<ByteArray>();

    } else if ("String".equals(keySerializer)) {
      kafkaSinkComponent = new KafkaSinkComponent<String>();

    } else if ("Integer".equals(keySerializer)) {
      kafkaSinkComponent = new KafkaSinkComponent<Integer>();

    } else if ("Long".equals(keySerializer)) {
      kafkaSinkComponent = new KafkaSinkComponent<Long>();

    } else {
      throw new IllegalArgumentException(
          "Key serializer for kafka sink is not supported: " + keySerializer);
    }
    return kafkaSinkComponent;
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
        "reconnect.backoff.ms", "retry.backoff.ms", "schema.registry.url",
        "serdes.protocol.version", "writer.schema.version"
    };
    String[] fieldNames = {
        "bootstrapServers", "bufferMemory", "compressionType", "retries", "batchSize", "clientId",
        "maxConnectionIdle",
        "lingerTime", "maxBlock", "maxRequestSize", "receiveBufferSize", "requestTimeout",
        "securityProtocol", "sendBufferSize",
        "timeout", "blocKOnBufferFull", "maxInflighRequests", "metadataFetchTimeout",
        "metadataMaxAge", "reconnectBackoff", "retryBackoff",
        TopologyLayoutConstants.SCHEMA_REGISTRY_URL, "serProtocolVersion", "writerSchemaVersion"
    };

    String securityProtocol = (String) conf.get("securityProtocol");
    if (!Strings.isNullOrEmpty(securityProtocol)) {
      producerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      producerProperties.put("sasl.mechanism", "PLAIN");
      System.setProperty("java.security.auth.login.config",
          "/Users/satendra.sahu/code/github/streamline/conf/jaas.conf");
    }

    for (int j = 0; j < propertyNames.length; ++j) {
      if (conf.get(fieldNames[j]) != null) {
        producerProperties.put(propertyNames[j], conf.get(fieldNames[j]));
      }
    }

    //add schemaRegistry url
    return producerProperties;
  }

  private Class getKeySerializer() {
    String keySerializer = (String) conf.get("keySerializer");
    if ((keySerializer == null) || "ByteArray".equals(keySerializer)) {
      return org.apache.kafka.common.serialization.ByteArraySerializer.class;
    } else if ("String".equals(keySerializer)) {
      return org.apache.kafka.common.serialization.StringSerializer.class;
    } else if ("Integer".equals(keySerializer)) {
      return org.apache.kafka.common.serialization.IntegerSerializer.class;
    } else if ("Long".equals(keySerializer)) {
      return org.apache.kafka.common.serialization.LongSerializer.class;
    } else {
      throw new IllegalArgumentException(
          "Key serializer for kafka sink is not supported: " + keySerializer);
    }
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

  private void setSaslJaasConfig() {
        /*String securityProtocol = (String) conf.get("securityProtocol");
        if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol.startsWith("SASL")) {
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
            conf.put(SASL_JAAS_CONFIG_KEY, saslConfigStrBuilder.toString());
        }*/

  }

  private void validateSSLConfig() {
        /*String securityProtocol = (String) conf.get("securityProtocol");
        if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol.endsWith("SSL")) {
            String truststoreLocation = (String) conf.get("sslTruststoreLocation");
            String truststorePassword = (String) conf.get("sslTruststorePassword");
            if (truststoreLocation == null || truststoreLocation.isEmpty()) {
                throw new IllegalArgumentException("Truststore location must be provided for SSL");
            }
            if (truststorePassword == null || truststorePassword.isEmpty()) {
                throw new IllegalArgumentException("Truststore password must be provided for SSL");
            }
        }*/
  }
}
