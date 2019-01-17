package com.hortonworks.streamline.streams.layout.beam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.beam.rule.BeamRuleTranslator;
import com.hortonworks.streamline.streams.layout.beam.rule.expression.BeamUtilFunctions;
import com.hortonworks.streamline.streams.layout.component.impl.RulesProcessor;
import com.hortonworks.streamline.streams.layout.component.rule.Rule;
import com.hortonworks.streamline.streams.layout.component.rule.expression.Window;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Created by Satendra Sahu on 12/12/18
 */
public class BeamWindowRuleComponent extends BeamRuleComponent {

  private String beamComponentId = "beamWindowRule" + UUID_FOR_COMPONENTS;

  public BeamWindowRuleComponent() {
  }

  @Override
  public void generateComponent(PCollection pCollection) {
    rulesProcessor = (RulesProcessor) conf
        .get(TopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY);

    //TODO add a mapping for type of windows in BEAM(UI change)
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

    PCollection<StreamlineEvent> windowedEvents = applyWindowing(pCollection);
    PCollection<StreamlineEvent> filteredEvents = windowedEvents;

    for (Rule windowRule : rulesProcessor.getRules()) {
      BeamRuleTranslator translator = new BeamRuleTranslator(beamComponentId, filteredEvents,
          windowRule.getCondition(),
          windowRule.getProjection(),
          windowRule.getGroupBy(),
          windowRule.getHaving());

      filteredEvents = translator.getpCollection();
    }

    if (outputCollection == null) {
      outputCollection = filteredEvents;
    } else {
      unionInputCollection(filteredEvents);
    }
  }


  //TODO implement session based windowing for BEAM Model
  private PCollection<StreamlineEvent> applyWindowing(
      PCollection<StreamlineEvent> inputCollection) {
    PCollection<StreamlineEvent> windowedEvents = null;

    Rule rule = rulesProcessor.getRules().get(0);
    rule.getId();
    Window window = rule.getWindow();
    String timestampField = window.getTsField();

    //Add timestamp to each event
    PCollection<StreamlineEvent> timestampedCollection = inputCollection
        .apply("AddEventTimestampParDo", BeamUtilFunctions.addTimestampToEvents(timestampField));

    //TODO add implementation for count based windowing
    //TODO add triggers to windows
    if (window.getWindowLength() instanceof Window.Duration) {
      Window.Duration windowLength = (Window.Duration) window.getWindowLength();
      Window.Duration slidingInterval = (Window.Duration) window.getSlidingInterval();

      int windowIntervalDurationSec = windowLength.getDurationMs() / 1000;
      int slidingIntervalDurationSec = slidingInterval.getDurationMs() / 1000;
      int lagInterval = window.getLagMs();

      //Fixed window
      if (slidingIntervalDurationSec <= 0) {
        windowedEvents = timestampedCollection.apply(
            org.apache.beam.sdk.transforms.windowing.Window.<StreamlineEvent>into(
                FixedWindows.of(Duration.standardSeconds(windowIntervalDurationSec)))
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1000)))
                .withAllowedLateness(Duration.standardSeconds(lagInterval))
                .discardingFiredPanes()
        );
      } else {
        //sliding window
        windowedEvents = timestampedCollection.apply(
            org.apache.beam.sdk.transforms.windowing.Window.<StreamlineEvent>into(
                SlidingWindows.of(Duration.standardSeconds(windowIntervalDurationSec))
                    .every(Duration.standardSeconds(slidingIntervalDurationSec)))
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1000)))
                .withAllowedLateness(Duration.standardSeconds(lagInterval))
                .discardingFiredPanes()

        );
      }
    }

    return windowedEvents;
  }
}
