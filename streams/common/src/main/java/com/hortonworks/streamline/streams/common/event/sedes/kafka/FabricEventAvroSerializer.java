/**
 * Copyright 2017 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/
package com.hortonworks.streamline.streams.common.event.sedes.kafka;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.Constants;
import com.hortonworks.streamline.streams.common.utils.FabricEventUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FabricEventAvroSerializer implements Serializer<StreamlineEvent> {

    protected static final Logger LOG = LoggerFactory.getLogger(FabricEventAvroSerializer.class);
    private final AvroSnapshotSerializer avroSnapshotSerializer;
    private SchemaRegistryClient schemaRegistryClient;
    private Integer writerSchemaVersion;

    public FabricEventAvroSerializer() {
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
        String schema = (String) streamlineEvent.getHeader().get(
            Constants.FABRIC_METADATA_SCHEMA);
        String tenant = (String) streamlineEvent.getHeader().get(Constants.FABRIC_METADATA_TENANT);
        writerSchemaVersion = (Integer) streamlineEvent.getHeader()
            .get(Constants.FABRIC_METADATA_SCHEMA_VERSION);

        SchemaMetadata schemaMetadata = getSchemaKey(schema, tenant, false);
        SchemaVersionInfo schemaVersionInfo;
        try {
            schemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schema)
                .getSchemaMetadata();
            if (writerSchemaVersion != null) {
                schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(
                    new SchemaVersionKey(schema, writerSchemaVersion));
            } else {
                schemaVersionInfo = schemaRegistryClient
                    .getLatestSchemaVersionInfo(schema);
            }
        } catch (SchemaNotFoundException e) {
            LOG.error("Exception occured while getting SchemaVersionInfo for " + schemaMetadata, e);
            throw new RuntimeException(e);
        }
        if (streamlineEvent == null || streamlineEvent.isEmpty()) {
            return null;
        } else {
            try {
                return avroSnapshotSerializer.serialize(FabricEventUtils.getAvroRecord(streamlineEvent,
                    new Schema.Parser().parse(schemaVersionInfo.getSchemaText())),
                    schemaMetadata);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    private SchemaMetadata getSchemaKey(String schema, String tenant, boolean isKey) {
        String name = isKey ? schema + ":k" : schema;
        return new SchemaMetadata.Builder(name).type(AvroSchemaProvider.TYPE).schemaGroup(tenant)
            .build();
    }

    @Override
    public void close() {
        try {
            avroSnapshotSerializer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
