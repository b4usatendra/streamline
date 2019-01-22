/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/
package com.hortonworks.streamline.streams.common.event.sedes.kafka;

import com.google.common.base.Preconditions;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamlineEventDeserializer implements Deserializer<StreamlineEvent> {

  protected static final Logger LOG = LoggerFactory.getLogger(StreamlineEventDeserializer.class);

  private final AvroStreamsSnapshotDeserializer avroStreamsSnapshotDeserializer;
  private Integer readerSchemaVersion;
  private Map<String, ?> configs;

  public StreamlineEventDeserializer() {
    avroStreamsSnapshotDeserializer = new AvroStreamsSnapshotDeserializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // ignoring the isKey since this class is expected to be used only as a value serializer for now, value being StreamlineEvent
    this.configs = configs;
    avroStreamsSnapshotDeserializer.init(configs);
    String readerSchemaVersion = (String) configs.get("reader.schema.version");
    if (readerSchemaVersion != null && !readerSchemaVersion.isEmpty()) {
      this.readerSchemaVersion = Integer.parseInt(readerSchemaVersion);
    } else {
      this.readerSchemaVersion = null;
    }

  }

  @Override
  public StreamlineEvent deserialize(String topic, byte[] bytes) {

    Map<String, Object> keyValues = (Map<String, Object>) avroStreamsSnapshotDeserializer
        .deserialize(new StreamlineEventDeserializer.ByteBufferInputStream(ByteBuffer.wrap(bytes)),
            readerSchemaVersion);
    StreamlineEvent streamlineEvent = StreamlineEventImpl.builder().putAll(keyValues).build();
    return streamlineEvent;

  }

  public static class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buf;

    public ByteBufferInputStream(ByteBuffer buf) {
      this.buf = buf;
    }

    public int read() throws IOException {
      if (!buf.hasRemaining()) {
        return -1;
      }
      return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) throws IOException {
      Preconditions.checkNotNull(bytes, "Given byte array can not be null");
      Preconditions.checkPositionIndexes(off, off + len, bytes.length);

      if (!buf.hasRemaining()) {
        return -1;
      }

      if (len == 0) {
        return 0;
      }

      int end = Math.min(len, buf.remaining());
      buf.get(bytes, off, end);
      return end;
    }
  }

  private SchemaMetadata getSchemaKey(String topic, boolean isKey) {
    String name = isKey ? topic + ":k" : topic;
    return new SchemaMetadata.Builder(name).type(AvroSchemaProvider.TYPE).schemaGroup("kafka")
        .build();
  }

  @Override
  public void close() {
    try {
      avroStreamsSnapshotDeserializer.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  //package level access for testing
  static Object getAvroRecord(StreamlineEvent streamlineEvent, Schema schema) {
    if (streamlineEvent.containsKey(StreamlineEvent.PRIMITIVE_PAYLOAD_FIELD)) {
      if (streamlineEvent.keySet().size() > 1) {
        throw new RuntimeException("Invalid schema, primitive schema can contain only one field.");
      }
      return streamlineEvent.get(StreamlineEvent.PRIMITIVE_PAYLOAD_FIELD);
    }
    GenericRecord result;
    result = new GenericData.Record(schema);
    for (Map.Entry<String, Object> entry : streamlineEvent.entrySet()) {
      result.put(entry.getKey(),
          getAvroValue(entry.getValue(), schema.getField(entry.getKey()).schema()));
    }
    return result;
  }

  private static Object getAvroValue(Object input, Schema schema) {
    if (input instanceof byte[] && Schema.Type.FIXED.equals(schema.getType())) {
      return new GenericData.Fixed(schema, (byte[]) input);
    } else if (input instanceof Map && !((Map) input).isEmpty()) {
      GenericRecord result;
      result = new GenericData.Record(schema);
      for (Map.Entry<String, Object> entry : ((Map<String, Object>) input).entrySet()) {
        result.put(entry.getKey(),
            getAvroValue(entry.getValue(), schema.getField(entry.getKey()).schema()));
      }
      return result;
    } else if (input instanceof Collection && !((Collection) input).isEmpty()) {
      // for array even though we(Schema in streamline registry) support different types of elements in an array, avro expects an array
      // schema to have elements of same type. Hence, for now we will restrict array to have elements of same type. Other option is convert
      // a  streamline Schema Array field to Record in avro. However, with that the issue is that avro Field constructor does not allow a
      // null name. We could potentiall hack it by plugging in a dummy name like arrayfield, but seems hacky so not taking that path
      List<Object> values = new ArrayList<>(((Collection) input).size());
      for (Object value : (Collection) input) {
        values.add(getAvroValue(value, schema.getElementType()));
      }
      return new GenericData.Array<Object>(schema, values);
    } else {
      return input;
    }
  }
}
