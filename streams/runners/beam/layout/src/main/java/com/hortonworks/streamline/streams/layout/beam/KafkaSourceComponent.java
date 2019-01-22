package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.event.sedes.kafka.KafkaDeserializer;
import com.hortonworks.streamline.streams.layout.component.StreamlineSource;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.io.kafka.KafkaIO;

/**
 * Created by Satendra Sahu on 12/5/18
 */
public class KafkaSourceComponent<K> extends StreamlineSource implements Serializable {

  private Class<K> type;

  //TODO change event timestamp #withTimestampPolicyFactory()
  public KafkaIO.Read<K, StreamlineEvent> getKafkaSource(Map<String, Object> conf,
      String bootStrapServers, String topics, Map<String, Object> consumerProperteis)
      throws ClassNotFoundException {
    String valueDeserializer = (String) conf.get("value.deserializer");
    String keyDeserializer = (String) conf.get("key.deserializer");
    return KafkaIO.<K, ByteBuffer>read()
        .withBootstrapServers(bootStrapServers)
        .withTopics(Arrays.asList((topics)))
        .withKeyDeserializer(KafkaDeserializer.getDeserializer(keyDeserializer))
        .withValueDeserializer(KafkaDeserializer.getDeserializer(valueDeserializer))
        .updateConsumerProperties(consumerProperteis);
  }
}