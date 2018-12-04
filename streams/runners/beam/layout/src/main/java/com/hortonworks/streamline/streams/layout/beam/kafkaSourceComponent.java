package com.hortonworks.streamline.streams.layout.beam;

import org.apache.beam.sdk.io.kafka.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.*;

/**
 * Created by Satendra Sahu on 12/4/18
 */
public class kafkaSourceComponent<K, V> {

    public PCollection<KV<K,V>> getKafkaSource(Map<String, Object> conf, String bootStrapServers, String topics, Properties consumerProperteis) {
        KafkaIO.<K, V>read()
                .withBootstrapServers(bootStrapServers)
                .withTopics(Arrays.asList(topics))
                .withKeyDeserializer(getKeyDeserializer((String) conf.get("")))
                .withValueDeserializer(getKeyDeserializer((String) conf.get("")))
                .updateConsumerProperties(consumerProperteis)
                .withoutMetadata();
    }


    private Class getKeyDeserializer(String keySerializer) {

        if ((keySerializer == null) || "ByteArray".equals(keySerializer)) {
            return org.apache.kafka.common.serialization.ByteArrayDeserializer.class;
        } else if ("String".equals(keySerializer)) {
            return org.apache.kafka.common.serialization.StringDeserializer.class;
        } else if ("Integer".equals(keySerializer)) {
            return org.apache.kafka.common.serialization.IntegerDeserializer.class;
        } else if ("Long".equals(keySerializer)) {
            return org.apache.kafka.common.serialization.LongDeserializer.class;
        } else {
            throw new IllegalArgumentException("Key serializer for kafka sink is not supported: " + keySerializer);
        }
    }
}
