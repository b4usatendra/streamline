package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.layout.beam.kafka.*;
import javassist.bytecode.*;
import org.apache.beam.sdk.io.kafka.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Created by Satendra Sahu on 12/5/18
 */
public class KafkaSourceComponent<K> implements Serializable {
    private Class<K> type;

    //TODO change event timestamp #withTimestampPolicyFactory()
    public KafkaIO.Read<K, StreamlineEvent> getKafkaSource(Map<String, Object> conf, String bootStrapServers, String topics, Map<String, Object> consumerProperteis) {
        return KafkaIO.<K, ByteBuffer>read()
                .withBootstrapServers(bootStrapServers)
                .withTopics(Arrays.asList((topics)))
                .withKeyDeserializer(getKeyDeserializer())
                .withValueDeserializer(StreamlineEventDeserializer.class)
                .updateConsumerProperties(consumerProperteis);
    }

    private Class getKeyDeserializer() {
        //TODO initialize if type is null
        if ((type == null || type == ByteArray.class)) {
            return org.apache.kafka.common.serialization.ByteArrayDeserializer.class;
        } else if (type == String.class) {
            return org.apache.kafka.common.serialization.StringDeserializer.class;
        } else if (type == Integer.class) {
            return org.apache.kafka.common.serialization.IntegerDeserializer.class;
        } else if (type == Long.class) {
            return org.apache.kafka.common.serialization.LongDeserializer.class;
        } else {
            throw new IllegalArgumentException("Key serializer for kafka sink is not supported: " + type);
        }
    }
}