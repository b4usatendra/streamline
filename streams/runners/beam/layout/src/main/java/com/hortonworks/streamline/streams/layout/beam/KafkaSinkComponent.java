package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.streams.StreamlineEvent;
import java.io.Serializable;
import java.util.Map;
import javassist.bytecode.ByteArray;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Created by Satendra Sahu on 12/4/18
 */
public class KafkaSinkComponent<K> implements Serializable {

  private Class<K> type;

  public KafkaIO.Write<K, StreamlineEvent> getKafkaSink(Map<String, Object> conf,
      String bootStrapServers, String topic, Map<String, Object> producerProperteis) {
    Serializer serializer = (Serializer) conf.get("value.serializer");
    return KafkaIO.<K, StreamlineEvent>write()
        .withBootstrapServers(bootStrapServers)
        .withTopic(topic)
        .withKeySerializer(getKeySerializer())
        .withValueSerializer(serializer.getClass())
        .updateProducerProperties(producerProperteis);

  }

  private Class getKeySerializer() {
    //TODO initialize if type is null
    if ((type == null || type == Long.class)) {
      return org.apache.kafka.common.serialization.ByteArraySerializer.class;
    } else if (type == String.class) {
      return org.apache.kafka.common.serialization.StringSerializer.class;
    } else if (type == Integer.class) {
      return org.apache.kafka.common.serialization.IntegerSerializer.class;
    } else if (type == ByteArray.class) {
      return org.apache.kafka.common.serialization.LongSerializer.class;
    } else {
      throw new IllegalArgumentException("Key serializer for kafka sink is not supported: " + type);
    }
  }
}
