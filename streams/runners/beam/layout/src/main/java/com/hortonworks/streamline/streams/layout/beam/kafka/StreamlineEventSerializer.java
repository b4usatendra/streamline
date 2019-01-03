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
package com.hortonworks.streamline.streams.layout.beam.kafka;

import com.hortonworks.registries.schemaregistry.*;
import com.hortonworks.registries.schemaregistry.avro.*;
import com.hortonworks.registries.schemaregistry.client.*;
import com.hortonworks.registries.schemaregistry.errors.*;
import com.hortonworks.registries.schemaregistry.serdes.avro.*;
import com.hortonworks.streamline.streams.*;
import org.apache.avro.*;
import org.apache.avro.generic.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import java.util.*;

public class StreamlineEventSerializer implements Serializer<StreamlineEvent> {
    protected static final Logger LOG = LoggerFactory.getLogger(StreamlineEventSerializer.class);
    private final AvroSnapshotSerializer avroSnapshotSerializer;
    private SchemaRegistryClient schemaRegistryClient;
    private Integer writerSchemaVersion;

    public StreamlineEventSerializer() {
        avroSnapshotSerializer = new AvroSnapshotSerializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // ignoring the isKey since this class is expected to be used only as a value serializer for now, value being StreamlineEvent
        avroSnapshotSerializer.init(configs);
        schemaRegistryClient = new SchemaRegistryClient(configs);
        String writerSchemaVersion = (String) configs.get("writer.schema.version");
        if (writerSchemaVersion != null && !writerSchemaVersion.isEmpty()) {
            this.writerSchemaVersion = Integer.parseInt(writerSchemaVersion);
        } else {
            this.writerSchemaVersion = null;
        }
    }

    @Override
    public byte[] serialize(String topic, StreamlineEvent streamlineEvent) {
        SchemaMetadata schemaMetadata = getSchemaKey(topic, false);
        SchemaVersionInfo schemaVersionInfo;
        try {
            schemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schemaMetadata.getName()).getSchemaMetadata();
            if (writerSchemaVersion != null) {
                schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaMetadata.getName(), writerSchemaVersion));
            } else {
                schemaVersionInfo = schemaRegistryClient.getLatestSchemaVersionInfo(schemaMetadata.getName());
            }
        } catch (SchemaNotFoundException e) {
            LOG.error("Exception occured while getting SchemaVersionInfo for " + schemaMetadata, e);
            throw new RuntimeException(e);
        }
        if (streamlineEvent == null || streamlineEvent.isEmpty()) {
            return null;
        } else {
            return avroSnapshotSerializer.serialize(getAvroRecord(streamlineEvent, new Schema.Parser().parse(schemaVersionInfo.getSchemaText())),
                    schemaMetadata);
        }
    }

    private SchemaMetadata getSchemaKey(String topic, boolean isKey) {
        String name = isKey ? topic + ":k" : topic;
        return new SchemaMetadata.Builder(name).type(AvroSchemaProvider.TYPE).schemaGroup("kafka").build();
    }

    @Override
    public void close() {
        try {
            avroSnapshotSerializer.close();
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
            try {
                result.put(entry.getKey(), getAvroValue(entry.getValue(), schema.getField(entry.getKey()).schema()));
            } catch (NullPointerException e) {
                LOG.info("Extraneous key: " + entry.getKey());
            }
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
                result.put(entry.getKey(), getAvroValue(entry.getValue(), schema.getField(entry.getKey()).schema()));
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
