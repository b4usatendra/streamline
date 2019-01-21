package com.hortonworks.streamline.streams.common.event.sedes.kafka;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;


/**
 * Created by Satendra Sahu on 1/16/19
 */
public enum KafkaSerializer {
  StringSerializer(StringSerializer.class),
  LongSerializer(LongSerializer.class),
  IntegerSerializer(IntegerSerializer.class),
  ByteArraySerializer(ByteArraySerializer.class),
  StreamlineAvroSerialzer(StreamlineEventSerializer.class),
  DoubleSerializer(DoubleSerializer.class);

  private Class serializer;

  KafkaSerializer(Class serializer) {
    this.serializer = serializer;
  }

  public static Class getSerializer(String name) {
    for (KafkaSerializer serializer : KafkaSerializer.values()) {
      if (serializer.name().equals(name)) {
        return serializer.serializer;
      }
    }
    return null;
  }
}
