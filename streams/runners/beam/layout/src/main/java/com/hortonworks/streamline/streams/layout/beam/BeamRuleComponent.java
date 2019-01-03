package com.hortonworks.streamline.streams.layout.beam;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.layout.*;
import com.hortonworks.streamline.streams.layout.component.impl.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.*;

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
        rulesProcessor = (RulesProcessor) conf.get(TopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);
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
        outputCollection = PCollectionList.of(outputCollection).and(collection).apply(Flatten.<StreamlineEvent>pCollections());
    }
}
