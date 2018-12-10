package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.layout.beam.kafka.*;
import javassist.bytecode.*;
import org.apache.beam.sdk.io.kafka.*;

import java.util.*;

/**
 * Created by Satendra Sahu on 12/4/18
 */
public class KafkaSinkComponent<K> {

    private Class<K> type;

    public KafkaIO.Write<K, StreamlineEvent> getKafkaSink(Map<String, Object> conf, String bootStrapServers, String topic, Map<String, Object> producerProperteis) {


        return KafkaIO.<K, StreamlineEvent>write()
                .withBootstrapServers(bootStrapServers)
                .withTopic(topic)
                .withKeySerializer(getKeySerializer())
                .withValueSerializer(StreamlineEventSerializer.class)
                .updateProducerProperties(producerProperteis);

    }

    private Class getKeySerializer() {
        //TODO initialize if type is null
        if ((type == null || type == ByteArray.class)) {
            return org.apache.kafka.common.serialization.ByteArraySerializer.class;
        } else if (type == String.class) {
            return org.apache.kafka.common.serialization.StringSerializer.class;
        } else if (type == Integer.class) {
            return org.apache.kafka.common.serialization.IntegerSerializer.class;
        } else if (type == Long.class) {
            return org.apache.kafka.common.serialization.LongSerializer.class;
        } else {
            throw new IllegalArgumentException("Key serializer for kafka sink is not supported: " + type);
        }
    }
}
