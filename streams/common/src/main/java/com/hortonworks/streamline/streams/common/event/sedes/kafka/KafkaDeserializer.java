package com.hortonworks.streamline.streams.common.event.sedes.kafka;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;


/**
 * Created by Satendra Sahu on 1/16/19
 */
public enum KafkaDeserializer {
  StringDeserializer(StringDeserializer.class),
  LongDeserializer(LongDeserializer.class),
  IntegerDeserializer(IntegerDeserializer.class),
  ByteArrayDeserializer(ByteArrayDeserializer.class),
  StreamlineAvroDeserialzer(StreamlineEventDeserializer.class),
  DoubleDeserializer(DoubleDeserializer.class),
  FabricEventJsonDeserializer(FabricEventJsonDeserializer.class);

  private Class deSerializer;

  KafkaDeserializer(Class deSerializer) {
    this.deSerializer = deSerializer;
  }

  public static Class getDeserializer(String name) {
    for (KafkaDeserializer serializer : KafkaDeserializer.values()) {
      if (serializer.name().equals(name)) {
        return serializer.deSerializer;
      }
    }
    return null;
  }
}
