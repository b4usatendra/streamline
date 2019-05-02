package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import com.hortonworks.streamline.streams.StreamlineEvent;
import java.util.Map;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Satendra Sahu on 4/26/19
 */
public class StreamlineToRowMapper {

    protected static final Logger LOG = LoggerFactory.getLogger(StreamlineToRowMapper.class);
    private final AvroSnapshotSerializer avroSnapshotSerializer;
    private SchemaRegistryClient schemaRegistryClient;

    public StreamlineToRowMapper() {
        avroSnapshotSerializer = new AvroSnapshotSerializer();
    }


    public void configure(Map<String, ?> configs, boolean isKey) {
        // ignoring the isKey since this class is expected to be used only as a value serializer for now, value being StreamlineEvent
        avroSnapshotSerializer.init(configs);
        schemaRegistryClient = new SchemaRegistryClient(configs);
    }

    public Row map(StreamlineEvent streamlineEvent) {
        SchemaMetadata schemaMetadata = getSchemaKey(streamlineEvent, false);
        int schemaVersion = (Integer) streamlineEvent.getHeader().get("schemaVersion");
        SchemaVersionInfo schemaVersionInfo;
        try {
            schemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schemaMetadata.getName())
                .getSchemaMetadata();

            schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(
                new SchemaVersionKey(schemaMetadata.getName(), schemaVersion));

        } catch (SchemaNotFoundException e) {
            LOG.error("Exception occured while getting SchemaVersionInfo for " + schemaMetadata, e);
            throw new RuntimeException(e);
        }
        return null;
    }

    private SchemaMetadata getSchemaKey(StreamlineEvent streamlineEvent, boolean isKey) {
        String name = (String) streamlineEvent.getHeader().get("schema");
        String tenant = (String) streamlineEvent.getHeader().get("tenant");
        return new SchemaMetadata.Builder(name).type(AvroSchemaProvider.TYPE)
            .schemaGroup(tenant).build();
    }

    private void generateRowSchema(String schemaText){

    }
}

