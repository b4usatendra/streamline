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
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroException;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroRetryableException;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.Constants;
import com.hortonworks.streamline.streams.common.FabricEventImpl;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FabricEventAvroDeserializer implements Deserializer<StreamlineEvent> {

    protected static final Logger LOG = LoggerFactory.getLogger(FabricEventAvroDeserializer.class);

    private final AvroStreamsSnapshotDeserializer avroStreamsSnapshotDeserializer;

    private Map<String, ?> configs;
    private SchemaRegistryClient schemaRegistryClient;

    public FabricEventAvroDeserializer() {
        avroStreamsSnapshotDeserializer = new AvroStreamsSnapshotDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // ignoring the isKey since this class is expected to be used only as a value serializer for now, value being StreamlineEvent
        this.configs = configs;
        avroStreamsSnapshotDeserializer.init(configs);
        schemaRegistryClient = new SchemaRegistryClient(configs);


    }

    @Override
    public StreamlineEvent deserialize(String topic, byte[] bytes) {

        return deserialize(topic,
            new FabricEventAvroDeserializer.ByteBufferInputStream(ByteBuffer.wrap(bytes)));
    }

    public StreamlineEvent deserialize(String topic, ByteBufferInputStream input
    ) throws SerDesException {

        // it can be enhanced to have respective protocol handlers for different versions
        byte protocolId = retrieveProtocolId(input);
        SchemaIdVersion schemaIdVersion = retrieveSchemaIdVersion(protocolId, input);
        SchemaVersionInfo schemaVersionInfo;
        SchemaMetadata schemaMetadata;
        try {
            schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(schemaIdVersion);
            schemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schemaVersionInfo.getName())
                .getSchemaMetadata();
        } catch (Exception e) {
            throw new RegistryException(e);
        }

        Map<String, Object> keyValues = (Map<String, Object>) avroStreamsSnapshotDeserializer
            .doDeserialize(input, protocolId, schemaMetadata, schemaVersionInfo.getVersion(),
                null);

        String id = (String) keyValues.get(Constants.FABRIC_ID);
        Map<String, Object> metadata = (Map<String, Object>) keyValues
            .get(Constants.FABRIC_METADATA);
        Map<String, Object> data = (Map<String, Object>) keyValues.get(Constants.FABRIC_DATA);

        Map<String, Object> newHeader = new HashMap<>();
        newHeader.putAll(metadata);
        newHeader.put(Constants.FABRIC_METADATA_SCHEMA, schemaMetadata.getName());
        newHeader.put(Constants.FABRIC_METADATA_SCHEMA_VERSION, schemaVersionInfo.getVersion());
        newHeader.put(Constants.FABRIC_METADATA_TENANT, schemaMetadata.getSchemaGroup());

        return FabricEventImpl.builder()
            .id(id)
            .dataSourceId((String) metadata.get(Constants.FABRIC_METADATA_SENDER))
            .sourceStream(topic)
            .header(newHeader)
            .putAll(data)
            .build();

    }

    private byte retrieveProtocolId(InputStream inputStream) throws SerDesException {
        // first byte is protocol version/id.
        // protocol format:
        // 1 byte  : protocol version
        byte protocolId;
        try {
            protocolId = (byte) inputStream.read();
        } catch (IOException e) {
            throw new AvroRetryableException(e);
        }

        if (protocolId == -1) {
            throw new AvroException("End of stream reached while trying to read protocol id");
        }

        checkProtocolHandlerExists(protocolId);

        return protocolId;
    }

    private void checkProtocolHandlerExists(byte protocolId) {
        if (SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler(protocolId) == null) {
            throw new AvroException("Unknown protocol id [" + protocolId
                + "] received while deserializing the payload");
        }
    }

    private SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, InputStream inputStream)
        throws SerDesException {
        return SerDesProtocolHandlerRegistry.get()
            .getSerDesProtocolHandler(protocolId)
            .handleSchemaVersionDeserialization(inputStream);
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

    @Override
    public void close() {
        try {
            avroStreamsSnapshotDeserializer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
