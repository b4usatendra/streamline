package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.streamline.common.exception.ComponentConfigException;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import org.apache.avro.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by Satendra Sahu on 1/10/19
 */
public class BeamORCSinkComponent extends AbstractBeamComponent {

  private ISchemaRegistryClient schemaRegistryClient;
  private static final Logger LOG = LoggerFactory.getLogger(BeamKafkaSinkComponent.class);

  @Override
  public PCollection<StreamlineEvent> getOutputCollection() {
    throw new NotImplementedException();
  }

  @Override
  public void generateComponent(PCollection<StreamlineEvent> pCollection) {
    String sinkId = "beamORCSink" + UUID_FOR_COMPONENTS;
    LOG.info("Generating BeamORCSinkComponent with id: ", sinkId);

    LOG.debug("Initialized with config: [{}]", conf);
    if (schemaRegistryClient == null) {
      schemaRegistryClient = new SchemaRegistryClient(conf);
    }

    pCollection.apply(ParDo.of(new DoFn<StreamlineEvent, StreamlineEvent>() {
      public void processElement(@Element StreamlineEvent streamlineEvent,
          OutputReceiver<StreamlineEvent> out) {
        String schemaName = (String) streamlineEvent.getHeader()
            .get(BeamTopologyLayoutConstants.SCHEMA_FIELD);
        String schemaVersion = (String) streamlineEvent.getHeader()
            .get(BeamTopologyLayoutConstants.VERSION_FIELD);
        String stream = (String) streamlineEvent.getHeader()
            .get(BeamTopologyLayoutConstants.TOPIC_FIELD);
        String tenant = (String) streamlineEvent.getHeader()
            .get(BeamTopologyLayoutConstants.TENANT_FIELD);
        try {
          SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(schemaName)
              .type(AvroSchemaProvider.TYPE).schemaGroup("kafka").build();
          schemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schemaMetadata.getName())
              .getSchemaMetadata();
          SchemaVersionInfo schemaVersionInfo = null;

          if (schemaVersion != null) {
            schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(
                new SchemaVersionKey(schemaMetadata.getName(), Integer.parseInt(schemaVersion)));
          } else {
            schemaVersionInfo = schemaRegistryClient
                .getLatestSchemaVersionInfo(schemaMetadata.getName());
          }

          Schema avroSchema = new Schema.Parser().parse(schemaVersionInfo.getSchemaText());


        } catch (SchemaNotFoundException e) {
          LOG.error("Exception occurred while getting SchemaVersionInfo for " + schemaName, e);
          throw new RuntimeException(e);
        }
      }
    }));


  }

  private SchemaMetadata getSchemaKey(String topic, boolean isKey) {
    String name = isKey ? topic + ":k" : topic;
    return new SchemaMetadata.Builder(name).type(AvroSchemaProvider.TYPE).schemaGroup("kafka")
        .build();
  }

  @Override
  public void validateConfig() throws ComponentConfigException {

  }
}
