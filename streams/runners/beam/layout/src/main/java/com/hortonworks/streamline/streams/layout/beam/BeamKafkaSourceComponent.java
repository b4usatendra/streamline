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
import com.hortonworks.streamline.streams.layout.component.*;
import com.hortonworks.streamline.streams.layout.component.impl.*;
import org.apache.beam.sdk.io.kafka.*;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.clients.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;
import sun.reflect.generics.reflectiveObjects.*;

import java.util.*;

/**
 * Implementation for KafkaSpout
 */
public class BeamKafkaSourceComponent extends AbstractBeamComponent
{

   private static final Logger LOG = LoggerFactory.getLogger(BeamKafkaSourceComponent.class);
   static final String SASL_JAAS_CONFIG_KEY = "saslJaasConfig";
   static final String SASL_KERBEROS_SERVICE_NAME = "kafkaServiceName";
   private KafkaSource kafkaSource;
   protected PCollection<KV<String, String>> outputCollection;

   // for unit tests
   public BeamKafkaSourceComponent()
   {
   }

   @Override
   public PCollection getOutputCollection()
   {
	  return outputCollection;
   }

   @Override
   public void unionInputCollection(PCollection collection)
   {
	  throw new NotImplementedException();
   }

   @Override
   public void generateComponent(PCollection inputCollection)
   {
	  if (!isGenerated)
	  {
		 StreamlineSource streamlineSource = (StreamlineSource) conf.get(BeamTopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);
		 kafkaSource = new KafkaSource();
		 kafkaSource.setConfig(streamlineSource.getConfig());
		 kafkaSource.setId(streamlineSource.getId());
		 kafkaSource.setName(streamlineSource.getName());
		 kafkaSource.setTopologyComponentBundleId(streamlineSource.getTopologyComponentBundleId());
		 kafkaSource.setTransformationClass(streamlineSource.getTransformationClass());
		 kafkaSource.addOutputStreams(streamlineSource.getOutputStreams());
		 //kafkaSource = (KafkaSource) conf.get(BeamTopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);
		 // add the output stream to conf so that the kafka spout declares output stream properly

		 if (kafkaSource != null && kafkaSource.getOutputStreams().size() == 1)
		 {
			conf.put(TopologyLayoutConstants.JSON_KEY_OUTPUT_STREAM_ID,
					kafkaSource.getOutputStreams().iterator().next().getId());
		 } else
		 {
			String msg = "Kafka source component [" + kafkaSource + "] should define exactly one output stream for Storm";
			LOG.error(msg, kafkaSource);
			throw new IllegalArgumentException(msg);
		 }
		 validateSSLConfig();
		 setSaslJaasConfig();

		 List<Object> configMethods = new ArrayList<>();
		 String[] configMethodNames = {
				 "withBootstrapServers", "withTopics"
		 };
		 String[] configKeys = {
				 "bootstrapServers", "topic"
		 };

		 configMethods.addAll(getConfigMethodsYaml(configMethodNames, configKeys));
		 String[] moreConfigMethodNames = {
				 "setRetry", "setRecordTranslator", "setProp"
		 };
		 String[] configMethodArgRefs = new String[moreConfigMethodNames.length];
		 String beamSourceId = "beamKafkaSource" + UUID_FOR_COMPONENTS;

		 initializeComponent(configKeys);
		 isGenerated = true;
	  }
   }

   private void initializeComponent(String[] configKeys)
   {
	  outputCollection = this.pipeline
			  .apply(KafkaIO.<String, String>read()
					  .withBootstrapServers((String) conf.get(configKeys[0]))
					  .withTopics(Arrays.asList((String) conf.get(configKeys[1])))
					  .withKeyDeserializer(StringDeserializer.class)
					  .withValueDeserializer(StringDeserializer.class)
					  .updateConsumerProperties(addConsumerProperties()).withoutMetadata());
   }

   private Map<String, Object> addConsumerProperties()
   {
	  String consumerPropertiesComponentId = "consumerProperties" + UUID_FOR_COMPONENTS;
	  Map<String, Object> consumerProperties = new HashMap<String, Object>();

	  String[] propertyNames = {
			  "group.id", "fetch.min.bytes", "max.partition.fetch.bytes", "max.poll.records", "security.protocol",
	  };
	  String[] fieldNames = {
			  "consumerGroupId", "fetchMinimumBytes", "fetchMaximumBytesPerPartition", "maxRecordsPerPoll", "securityProtocol",
	  };

	  consumerProperties.put("sasl.mechanism", "PLAIN");
	  consumerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

	  for (int j = 0; j < propertyNames.length; ++j)
	  {
		 if (conf.get(fieldNames[j]) != null)
		 {
			consumerProperties.put(propertyNames[j], conf.get(fieldNames[j]));
		 }
	  }

	  return consumerProperties;
   }

   private void setSaslJaasConfig()
   {
	  String securityProtocol = (String) conf.get("securityProtocol");
	  if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol.startsWith("SASL_PLAINTEXT"))
	  {
		/* StringBuilder saslConfigStrBuilder = new StringBuilder();
		 String kafkaServiceName = (String) conf.get(SASL_KERBEROS_SERVICE_NAME);
		 String principal = (String) conf.get("principal");
		 String keytab = (String) conf.get("keytab");
		 if (kafkaServiceName == null || kafkaServiceName.isEmpty())
		 {
			throw new IllegalArgumentException("Kafka service name must be provided for SASL GSSAPI Kerberos");
		 }
		 if (principal == null || principal.isEmpty())
		 {
			throw new IllegalArgumentException("Kafka client principal must be provided for SASL GSSAPI Kerberos");
		 }
		 if (keytab == null || keytab.isEmpty())
		 {
			throw new IllegalArgumentException("Kafka client principal keytab must be provided for SASL GSSAPI Kerberos");
		 }
		 saslConfigStrBuilder.append("com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"");
		 saslConfigStrBuilder.append(keytab).append("\"  principal=\"").append(principal).append("\";");
		 conf.put(SASL_JAAS_CONFIG_KEY, saslConfigStrBuilder.toString());*/
	  }
   }

   private void validateSSLConfig()
   {
	  /*String securityProtocol = (String) conf.get("securityProtocol");
	  if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol.endsWith("SASL_PLAINTEXT"))
	  {
		 String truststoreLocation = (String) conf.get("sslTruststoreLocation");
		 String truststorePassword = (String) conf.get("sslTruststorePassword");
		 if (truststoreLocation == null || truststoreLocation.isEmpty())
		 {
			throw new IllegalArgumentException("Truststore location must be provided for SSL");
		 }
		 if (truststorePassword == null || truststorePassword.isEmpty())
		 {
			throw new IllegalArgumentException("Truststore password must be provided for SSL");
		 }
	  }*/
   }
}
