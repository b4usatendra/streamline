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

import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.layout.*;
import com.hortonworks.streamline.streams.layout.component.*;
import com.hortonworks.streamline.streams.layout.component.impl.*;
import javassist.bytecode.*;
import joptsimple.internal.*;
import org.apache.beam.sdk.io.kafka.*;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.clients.*;
import org.slf4j.*;
import sun.reflect.generics.reflectiveObjects.*;

import java.util.*;

/**
 * Implementation for KafkaSpout
 */
public class BeamKafkaSourceComponent extends AbstractBeamComponent {

    static final String SASL_JAAS_CONFIG_KEY = "saslJaasConfig";
    static final String SASL_KERBEROS_SERVICE_NAME = "kafkaServiceName";
    private static final Logger LOG = LoggerFactory.getLogger(BeamKafkaSourceComponent.class);
    protected PCollection<KV<Object, StreamlineEvent>> outputCollection;
    private KafkaSource kafkaSource;
    private KafkaSourceComponent kafkaSourceComponent;

    // for unit tests
    public BeamKafkaSourceComponent() {
    }

    @Override
    public PCollection getOutputCollection() {
        return outputCollection;
    }

    @Override
    public void unionInputCollection(PCollection collection) {
        throw new NotImplementedException();
    }

    @Override
    public void generateComponent(PCollection inputCollection) {
        if (!isGenerated) {
            StreamlineSource streamlineSource = (StreamlineSource) conf.get(TopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);
            kafkaSource = new KafkaSource();
            kafkaSource.setConfig(streamlineSource.getConfig());
            kafkaSource.setId(streamlineSource.getId());
            kafkaSource.setName(streamlineSource.getName());
            kafkaSource.setTopologyComponentBundleId(streamlineSource.getTopologyComponentBundleId());
            kafkaSource.setTransformationClass(streamlineSource.getTransformationClass());
            kafkaSource.addOutputStreams(streamlineSource.getOutputStreams());
            //kafkaSource = (KafkaSource) conf.get(BeamTopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);
            // add the output stream to conf so that the kafka spout declares output stream properly

            if (kafkaSource != null && kafkaSource.getOutputStreams().size() == 1) {
                conf.put(TopologyLayoutConstants.JSON_KEY_OUTPUT_STREAM_ID,
                        kafkaSource.getOutputStreams().iterator().next().getId());
            } else {
                String msg = "Kafka source component [" + kafkaSource + "] should define exactly one output stream for Storm";
                LOG.error(msg, kafkaSource);
                throw new IllegalArgumentException(msg);
            }
            validateSSLConfig();
            setSaslJaasConfig();

            String beamSourceId = "beamKafkaSource" + UUID_FOR_COMPONENTS;

            String outputStream = (String) conf.get(TopologyLayoutConstants.JSON_KEY_OUTPUT_STREAM_ID);
            String topic = (String) conf.get(TopologyLayoutConstants.JSON_KEY_TOPIC);
            String sourceId = streamlineSource.getId();


            kafkaSourceComponent = getComponent();
            initializeComponent();
            isGenerated = true;
        }
    }

    //TODO create constansts class for all kafka related constants
    private void initializeComponent() {

        KafkaIO.Read<Object, StreamlineEvent> reader = kafkaSourceComponent
                .getKafkaSource(conf,
                        (String) conf.get("bootstrapServers"),
                        (String) conf.get("topic"), addConsumerProperties());
        outputCollection = this.pipeline.apply(reader.withoutMetadata());
    }


    //TODO add keyDeserializer/keySerializer property at component level
    private KafkaSourceComponent getComponent() {
        String keySerializer = (String) conf.get("keyDeserializer");
        KafkaSourceComponent kafkaSourceComponent = null;
        if ((keySerializer == null) || "ByteArray".equals(keySerializer)) {
            kafkaSourceComponent = new KafkaSourceComponent<ByteArray>();

        } else if ("String".equals(keySerializer)) {
            kafkaSourceComponent = new KafkaSourceComponent<String>();

        } else if ("Integer".equals(keySerializer)) {
            kafkaSourceComponent = new KafkaSourceComponent<Integer>();

        } else if ("Long".equals(keySerializer)) {
            kafkaSourceComponent = new KafkaSourceComponent<Long>();

        } else {
            throw new IllegalArgumentException("Key serializer for kafka sink is not supported: " + keySerializer);
        }
        return kafkaSourceComponent;
    }

    private Map<String, Object> addConsumerProperties() {
        String consumerPropertiesComponentId = "consumerProperties" + UUID_FOR_COMPONENTS;
        Map<String, Object> consumerProperties = new HashMap<String, Object>();

        String[] propertyNames = {
                "group.id", "fetch.min.bytes", "max.partition.fetch.bytes", "max.poll.records", "security.protocol", "schema.registry.url",
                "reader.schema.version"

        };
        String[] fieldNames = {
                "consumerGroupId", "fetchMinimumBytes", "fetchMaximumBytesPerPartition", "maxRecordsPerPoll", "securityProtocol", "schemaRegistryUrl"
                ,"readerSchemaVersion"
        };



        /*String securityProtocol =  (String)conf.get("securityProtocol");

        //TODO create JAAS file based on the logged in user
        if(!Strings.isNullOrEmpty(securityProtocol)){
            consumerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,securityProtocol);
            consumerProperties.put("sasl.mechanism", "PLAIN");
            System.setProperty("java.security.auth.login.config", "/Users/satendra.sahu/code/github/streamline/conf/jaas.conf");
        }*/

        for (int j = 0; j < propertyNames.length; ++j) {
            if (conf.get(fieldNames[j]) != null) {
                consumerProperties.put(propertyNames[j], conf.get(fieldNames[j]));
            }
        }

        return consumerProperties;
    }

    private void setSaslJaasConfig() {
        String securityProtocol = (String) conf.get("securityProtocol");
        if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol.startsWith("SASL_PLAINTEXT")) {
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

    private void validateSSLConfig() {
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
