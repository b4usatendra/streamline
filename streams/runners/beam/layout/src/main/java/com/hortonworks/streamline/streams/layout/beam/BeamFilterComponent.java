package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.streamline.common.exception.ComponentConfigException;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.Constants;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.component.impl.CustomProcessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import joptsimple.internal.Strings;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Satendra Sahu on 12/11/18
 */
public class BeamFilterComponent extends AbstractBeamComponent {

    private static final Set<String> metadataKeySet = new HashSet(Arrays.asList(
        Constants.FABRIC_METADATA_SCHEMA));

    public final transient Logger log = LoggerFactory.getLogger(BeamFilterComponent.class);
    protected PCollection<StreamlineEvent> outputCollection;
    protected CustomProcessor filterProcessor;
    private SchemaRegistryClient schemaRegistryClient;
    private Map<String, org.apache.beam.sdk.schemas.Schema> rowSchemaCache = new HashMap<>();

    public BeamFilterComponent() {
    }

    @Override
    public PCollection getOutputCollection() {
        return outputCollection;
    }

    @Override
    public void generateComponent(PCollection pCollection) {

        filterProcessor = (CustomProcessor) conf
            .get(TopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);

        String beamFilterComponentId = "filterBeam" + UUID_FOR_COMPONENTS;

        initializeComponent(pCollection);
    }

    private void initializeComponent(PCollection<StreamlineEvent> pCollection) {
        org.apache.beam.sdk.schemas.Schema rowSchema;

        //TODO add filter condition.
        String keyFilter = (String) conf.get("keyFilter");
        int expectedValue = (int) conf.get("valueFilter");

        PCollection<StreamlineEvent> filteredEvents = pCollection
            .apply(ParDo.of(new DoFn<StreamlineEvent, StreamlineEvent>() {
                @ProcessElement
                public void processElement(@Element StreamlineEvent streamlineEvent,
                    OutputReceiver<StreamlineEvent> out) {

                    String currentSchema = (String) streamlineEvent.getHeader()
                        .get(Constants.FABRIC_METADATA_SCHEMA);

                    int currentSchemaVersion = (int) streamlineEvent.getHeader()
                        .get(Constants.FABRIC_METADATA_SCHEMA_VERSION);

                    boolean isMatched = false;
                    String actualValue = null;

                    if (metadataKeySet.contains(keyFilter)) {
                        actualValue = (String) streamlineEvent.getHeader().get(keyFilter);


                    } else {
                        actualValue = (String) streamlineEvent.getFieldsAndValues()
                            .get(keyFilter);
                    }

                    if (!Strings.isNullOrEmpty(actualValue) && actualValue.equals(expectedValue)) {
                        isMatched = true;
                    }

                    if (isMatched) {
                        out.output(streamlineEvent);
                    }
                }
            }));

        if (outputCollection == null) {
            outputCollection = filteredEvents;
        } else {
            unionInputCollection(filteredEvents);
        }
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
