/**
  * Copyright 2017 Hortonworks.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at

  *   http://www.apache.org/licenses/LICENSE-2.0

  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
 **/

package com.hortonworks.streamline.streams.layout.beam.kafka;

import com.google.common.collect.*;
import com.hortonworks.registries.schemaregistry.*;
import com.hortonworks.registries.schemaregistry.serde.*;
import com.hortonworks.registries.schemaregistry.serdes.avro.*;
import com.hortonworks.streamline.streams.*;
import org.apache.avro.*;
import org.apache.avro.generic.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 *
 */
public class AvroStreamsSnapshotDeserializer extends AvroSnapshotDeserializer {

    protected Object doDeserialize(InputStream payloadInputStream,
                                   byte protocolId,
                                   SchemaMetadata schemaMetadata,
                                   Integer writerSchemaVersion,
                                   Integer readerSchemaVersion) throws SerDesException {
        Object deserializedObj = super.doDeserialize(payloadInputStream, protocolId, schemaMetadata, writerSchemaVersion, readerSchemaVersion);

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        Object values = convertValue(deserializedObj);
        if (values instanceof Map) {
            builder.putAll((Map) values);
        } else {
            builder.put(StreamlineEvent.PRIMITIVE_PAYLOAD_FIELD, values);
        }

        return builder.build();
    }

    private Object convertValue(Object deserializedObj) {
        Object value;

        //check for specific-record type and build a map from that
        if (deserializedObj instanceof IndexedRecord) { // record
            IndexedRecord indexedRecord = (IndexedRecord) deserializedObj;
            List<Schema.Field> fields = indexedRecord.getSchema().getFields();
            ImmutableMap.Builder<String, Object> keyValues = ImmutableMap.builder();
            for (Schema.Field field : fields) {
                Object currentValue = convertValue(indexedRecord.get(field.pos()));
                if (currentValue != null) {
                    keyValues.put(field.name(), currentValue);
                }
            }
            value = keyValues.build();

        } else if (deserializedObj instanceof ByteBuffer) { // byte array representation
            ByteBuffer byteBuffer = (ByteBuffer) deserializedObj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            value = bytes;

        } else if (deserializedObj instanceof GenericEnumSymbol) { //enums
            GenericEnumSymbol symbol = (GenericEnumSymbol) deserializedObj;
            value = symbol.toString();

        } else if (deserializedObj instanceof CharSequence) { // symbols
            value = deserializedObj.toString();

        } else if (deserializedObj instanceof Map) { // type of map
            Map<Object, Object> map = (Map<Object, Object>) deserializedObj;
            ImmutableMap.Builder<String, Object> keyValues = ImmutableMap.builder();
            for (Map.Entry entry : map.entrySet()) {
                Object currentValue = convertValue(entry.getValue());
                if (currentValue != null) {
                    keyValues.put(entry.getKey().toString(), currentValue);
                }
            }
            value = keyValues.build();

        } else if (deserializedObj instanceof Collection) { // type of array
            Collection<Object> collection = (Collection<Object>) deserializedObj;
            ImmutableList.Builder<Object> values = ImmutableList.builder();
            for (Object obj : collection) {
                Object currentValue = convertValue(obj);
                if (currentValue != null) {
                    values.add(currentValue);
                }
            }
            value = values.build();

        } else if (deserializedObj instanceof GenericFixed) { // fixed type
            GenericFixed genericFixed = (GenericFixed) deserializedObj;
            value = genericFixed.bytes();

        } else { // other primitive types
            value = deserializedObj;
        }

        return value;
    }

}
