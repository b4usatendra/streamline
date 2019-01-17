package com.hortonworks.streamline.streams.layout.beam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.streamline.common.exception.ComponentConfigException;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.component.impl.RulesProcessor;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Satendra Sahu on 12/11/18
 */
public class BeamRuleComponent extends AbstractBeamComponent {

  public final transient Logger log = LoggerFactory.getLogger(BeamRuleComponent.class);
  protected PCollection<StreamlineEvent> outputCollection;
  protected RulesProcessor rulesProcessor;

  public BeamRuleComponent() {
  }

  @Override
  public PCollection getOutputCollection() {
    return outputCollection;
  }

  @Override
  public void generateComponent(PCollection pCollection) {
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
