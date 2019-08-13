package com.hortonworks.streamline.streams.layout.beam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.streamline.common.exception.ComponentConfigException;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.Constants;
import com.hortonworks.streamline.streams.common.utils.FabricEventUtils;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.component.impl.RulesProcessor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Satendra Sahu on 12/11/18
 */
public class BeamRuleComponent extends AbstractBeamComponent {

    public final transient Logger log = LoggerFactory.getLogger(BeamRuleComponent.class);
    protected PCollection<StreamlineEvent> outputCollection;
    protected RulesProcessor rulesProcessor;
    private SchemaRegistryClient schemaRegistryClient;
    private Map<String, org.apache.beam.sdk.schemas.Schema> rowSchemaCache = new HashMap<>();

    public BeamRuleComponent() {
    }

    @Override
    public PCollection getOutputCollection() {
        return outputCollection;
    }

    @Override
    public void generateComponent(PCollection pCollection) {

        schemaRegistryClient = new SchemaRegistryClient(conf);

        rulesProcessor = (RulesProcessor) conf
            .get(TopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);
        String beamRuleComponentId = "ruleBeam" + UUID_FOR_COMPONENTS;

        ObjectMapper mapper = new ObjectMapper();
        String rulesProcessorJson = null;
        try {
            rulesProcessorJson = mapper.writeValueAsString(rulesProcessor);
        } catch (JsonProcessingException e) {
            log.error("Error creating json config string for RulesProcessor",
                e);
        }

        initializeComponent(pCollection);
    }

    private void initializeComponent(PCollection<StreamlineEvent> pCollection) {
        org.apache.beam.sdk.schemas.Schema rowSchema;

        //TODO add filter condition.
        String filterSchemaName = (String) conf.get("schemaName");
        int filterSchemaVersion = (int) conf.get("schemaVersion");

        PCollection<StreamlineEvent> filteredPCollection = pCollection
            .apply(ParDo.of(new DoFn<StreamlineEvent, StreamlineEvent>() {
                @ProcessElement
                public void processElement(@Element StreamlineEvent streamlineEvent,
                    OutputReceiver<StreamlineEvent> out) {

                    String currentSchema = (String) streamlineEvent.getHeader()
                        .get(Constants.FABRIC_METADATA_SCHEMA);

                    int currentSchemaVersion = (int) streamlineEvent.getHeader()
                        .get(Constants.FABRIC_METADATA_SCHEMA_VERSION);

                    if (filterSchemaName.equals(currentSchema)
                        && filterSchemaVersion == currentSchemaVersion) {
                        out.output(streamlineEvent);
                    }
                }
            }));

        PCollection<Row> rowPCollection = filteredPCollection
            .apply(ParDo.of(new DoFn<StreamlineEvent, Row>() {
                @ProcessElement
                public void processElement(ProcessContext c) {

                    StreamlineEvent streamlineEvent = c.element();

                    String tenant = (String) streamlineEvent.getHeader()
                        .get(Constants.FABRIC_METADATA_TENANT);

                    String schemaName = getSchemaKey(filterSchemaName, tenant, false).getName();
                    Schema avroSchema = getAvroSchema(streamlineEvent);

                    GenericRecord record = null;
                    try {
                        record = (GenericRecord) FabricEventUtils
                            .getAvroRecord(streamlineEvent, avroSchema);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    org.apache.beam.sdk.schemas.Schema rowSchema;
                    if (rowSchemaCache.containsKey(schemaName)) {
                        rowSchema = rowSchemaCache.get(schemaName);
                    } else {
                        rowSchema = AvroUtils.toSchema(avroSchema);
                        rowSchemaCache.put(schemaName, rowSchema);

                    }
                    c.output(AvroUtils.toRowStrict(record, rowSchema));
                }
            }));

        rowPCollection
            .setRowSchema(new org.apache.beam.sdk.schemas.Schema(Collections.emptyList()));
        PCollection<Row> result = rowPCollection
            .apply(SqlTransform.query("SELECT * FROM PCOLLECTION"));

        outputCollection = result.apply(ParDo.of(new DoFn<Row, StreamlineEvent>() {
            @ProcessElement
            public void processElement(@Element Row row,
                OutputReceiver<StreamlineEvent> out) {
                org.apache.beam.sdk.schemas.Schema schema = row.getSchema();
                schema.getFieldNames();

                //out.output(StreamlineEvent);
            }
        }));


    }


    private org.apache.avro.Schema getAvroSchema(StreamlineEvent streamlineEvent) {

        String schema = (String) streamlineEvent.getHeader().get(
            Constants.FABRIC_METADATA_SCHEMA);
        String tenant = (String) streamlineEvent.getHeader().get(Constants.FABRIC_METADATA_TENANT);
        Integer readerSchemaVersion = (Integer) streamlineEvent.getHeader()
            .get(Constants.FABRIC_METADATA_SCHEMA_VERSION);

        SchemaMetadata schemaMetadata = getSchemaKey(schema, tenant, false);

        SchemaVersionInfo schemaVersionInfo;
        try {
            schemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schema)
                .getSchemaMetadata();
            if (readerSchemaVersion != null) {
                schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(
                    new SchemaVersionKey(schema, readerSchemaVersion));
            } else {
                schemaVersionInfo = schemaRegistryClient
                    .getLatestSchemaVersionInfo(schema);
            }
        } catch (SchemaNotFoundException e) {
            log.error("Exception occured while getting SchemaVersionInfo for " + schemaMetadata, e);
            throw new RuntimeException(e);
        }

        return new org.apache.avro.Schema.Parser().parse(schemaVersionInfo.getSchemaText());
    }

    private SchemaMetadata getSchemaKey(String schema, String tenant, boolean isKey) {
        String name = isKey ? schema + ":k" : schema;
        return new SchemaMetadata.Builder(name).type(AvroSchemaProvider.TYPE).schemaGroup(tenant)
            .build();
    }

    @Override
    public void unionInputCollection(PCollection<StreamlineEvent> collection) {
        outputCollection = PCollectionList.of(outputCollection).and(collection)
            .apply(Flatten.<StreamlineEvent>pCollections());
    }

    @Override
    public void validateConfig() throws ComponentConfigException {

    }
}
