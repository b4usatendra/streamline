/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.streams.layout.*;
import org.apache.beam.sdk.io.kafka.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.clients.*;
import org.slf4j.*;
import sun.reflect.generics.reflectiveObjects.*;

import java.util.*;

/**
 * Implementation for Beam Kafka Producer
 */
public class BeamKafkaSinkComponent extends AbstractBeamComponent
{
   private static final Logger LOG = LoggerFactory.getLogger(BeamKafkaSinkComponent.class);
   static final String SASL_JAAS_CONFIG_KEY = "saslJaasConfig";
   static final String SASL_KERBEROS_SERVICE_NAME = "kafkaServiceName";
   protected PCollection<KV<String, String>> inputCollection;
   protected PCollection<KV<String, String>> outputCollection;

   public BeamKafkaSinkComponent(){}

   @Override
   public PCollection getOutputCollection()
   {
	  throw new NotImplementedException();
   }

   @Override
   public void unionInputCollection(PCollection<KV<String, String>> collection)
   {
	  outputCollection = PCollectionList.of(outputCollection).and(collection).apply(Flatten.<KV<String, String>>pCollections());
   }

   @Override
   public void generateComponent(PCollection pCollection)
   {
	  validateSSLConfig();
	  setSaslJaasConfig();
	  String sinkId = "beamKafkaSink" + UUID_FOR_COMPONENTS;
	  String[] configKeys = {
			  "bootstrapServers", "topic"
	  };
	  initializeComponent(pCollection, configKeys);
   }

   private void initializeComponent(PCollection<KV<String, String>> pCollection, String[] configKeys)
   {
	  pCollection.apply(KafkaIO.<String, String>write()
			  .withBootstrapServers((String) conf.get(configKeys[0]))
			  .withTopic((String) conf.get(configKeys[1]))
			  .withKeySerializer(org.apache.kafka.common.serialization.StringSerializer.class)
			  .withValueSerializer(org.apache.kafka.common.serialization.StringSerializer.class)
			  .updateProducerProperties(addProducerProperties()));

	  if (outputCollection == null)
	  {
		 outputCollection = pCollection;
	  } else
	  {
		 unionInputCollection(pCollection);
	  }
   }

   private Map<String, Object> addProducerProperties()
   {
	  String producerPropertiesComponentId = "producerProperties" + UUID_FOR_COMPONENTS;

	  Map<String, Object> producerProperties = new HashMap<>();

	  //fieldNames and propertyNames arrays should be of same length
	  String[] propertyNames = {
			  "bootstrap.servers", "buffer.memory", "compression.type", "retries", "batch.size", "client.id", "connections.max.idle.ms",
			  "linger.ms", "max.block.ms", "max.request.size", "receive.buffer.bytes", "request.timeout.ms", "security.protocol", "send.buffer.bytes",
			  "timeout.ms", "block.on.buffer.full", "max.in.flight.requests.per.connection", "metadata.fetch.timeout.ms", "metadata.max.age.ms",
			  "reconnect.backoff.ms", "retry.backoff.ms", "schema.registry.url", "serdes.protocol.version", "writer.schema.version"
	  };
	  String[] fieldNames = {
			  "bootstrapServers", "bufferMemory", "compressionType", "retries", "batchSize", "clientId", "maxConnectionIdle",
			  "lingerTime", "maxBlock", "maxRequestSize", "receiveBufferSize", "requestTimeout", "securityProtocol", "sendBufferSize",
			  "timeout", "blocKOnBufferFull", "maxInflighRequests", "metadataFetchTimeout", "metadataMaxAge", "reconnectBackoff", "retryBackoff",
			  TopologyLayoutConstants.SCHEMA_REGISTRY_URL, "serProtocolVersion", "writerSchemaVersion"
	  };

	  producerProperties.put("sasl.mechanism", "PLAIN");
	  producerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
	  for (int j = 0; j < propertyNames.length; ++j)
	  {
		 if (conf.get(fieldNames[j]) != null)
		 {
			producerProperties.put(propertyNames[j], conf.get(fieldNames[j]));
		 }
	  }
	  return producerProperties;
   }

   private Class getKeySerializer()
   {
	  String keySerializer = (String) conf.get("keySerializer");
	  if ((keySerializer == null) || "ByteArray".equals(keySerializer))
	  {
		 return org.apache.kafka.common.serialization.ByteArraySerializer.class;
	  } else if ("String".equals(keySerializer))
	  {
		 return org.apache.kafka.common.serialization.StringSerializer.class;
	  } else if ("Integer".equals(keySerializer))
	  {
		 return org.apache.kafka.common.serialization.IntegerSerializer.class;
	  } else if ("Long".equals(keySerializer))
	  {
		 return org.apache.kafka.common.serialization.LongSerializer.class;
	  } else
	  {
		 throw new IllegalArgumentException("Key serializer for kafka sink is not supported: " + keySerializer);
	  }
   }

   private String getAckMode()
   {
	  String ackMode = (String) conf.get("ackMode");
	  if ("None".equals(ackMode))
	  {
		 return "0";
	  } else if ("Leader".equals(ackMode) || (ackMode == null))
	  {
		 return "1";
	  } else if ("All".equals(ackMode))
	  {
		 return "all";
	  } else
	  {
		 throw new IllegalArgumentException("Ack mode for kafka sink is not supported: " + ackMode);
	  }
   }

   private void setSaslJaasConfig()
   {
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

   private void validateSSLConfig()
   {
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
