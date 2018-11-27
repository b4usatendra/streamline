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
package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.common.Config;
import com.hortonworks.streamline.streams.layout.component.*;
import com.hortonworks.streamline.streams.layout.component.impl.RulesProcessor;
import com.hortonworks.streamline.streams.layout.component.rule.Rule;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotSupportedException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class BeamTopologyFluxGenerator extends TopologyDagVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(BeamTopologyFluxGenerator.class);
    private static final int DELTA = 5;
    private final BeamFluxComponentFactory fluxComponentFactory;
    private final List<Map.Entry<String, Component>> keysAndComponents = new ArrayList<>();
    private final TopologyDag topologyDag;
    private final Map<String, Object> config;
    private final Config topologyConfig;
    private final Set<String> edgeAlreadyAddedComponents = new HashSet<>();
    private Pipeline pipeline = Pipeline.create();
    private Map<String, BeamComponent> componentMap = new HashMap<String, BeamComponent>();
    private Set<String> visitedComponent = new HashSet<>();

    public BeamTopologyFluxGenerator(TopologyLayout topologyLayout, Map<String, Object> config, Path extraJarsLocation) {
        this.topologyDag = topologyLayout.getTopologyDag();
        this.topologyConfig = topologyLayout.getConfig();
        this.config = config;
        fluxComponentFactory = new BeamFluxComponentFactory(extraJarsLocation);
    }

    //TODO add stage to the pipeline
    @Override
    public void visit(StreamlineSource source) {
        String sourceId = getFluxId(source);
        if (!componentMap.containsKey(sourceId)) {
            BeamComponent beamSourceComponent = fluxComponentFactory.getFluxComponent(source);
            getYamlComponents(beamSourceComponent, source);
            beamSourceComponent.generateComponent(null);
            componentMap.put(sourceId, beamSourceComponent);
            visitedComponent.add(sourceId);
        }
	  /*keysAndComponents.add(makeEntry(BeamTopologyLayoutConstants.YAML_KEY_BEAM_SOURCE,
			  beamSourceComponent));*/
    }

    @Override
    public void visit(StreamlineSink sink) {
        String sinkId = getFluxId(sink);
        if (!componentMap.containsKey(sinkId)) {
            BeamComponent beamSinkComponent = fluxComponentFactory.getFluxComponent(sink);
            getYamlComponents(beamSinkComponent, sink);
            componentMap.put(sinkId, beamSinkComponent);
            visitedComponent.add(sinkId);
        }

	  /*keysAndComponents.add(makeEntry(BeamTopologyLayoutConstants.YAML_KEY_BEAM_SINK,
			  getYamlComponents(fluxComponentFactory.getFluxComponent(sink), sink)));*/
    }

    @Override
    public void visit(StreamlineProcessor processor) {
        String processorId = getFluxId(processor);
        if (!componentMap.containsKey(processorId)) {
            BeamComponent beamProcessorComponent = fluxComponentFactory.getFluxComponent(processor);
            getYamlComponents(beamProcessorComponent, processor);
            componentMap.put(processorId, beamProcessorComponent);
            visitedComponent.add(processorId);
        }

	  /*keysAndComponents.add(makeEntry(BeamTopologyLayoutConstants.YAML_KEY_BEAM_PROCESSOR,
			  getYamlComponents(fluxComponentFactory.getFluxComponent(processor), processor)));*/
    }

    @Override
    public void visit(final RulesProcessor rulesProcessor) {
        rulesProcessor.getConfig().setAny("outputStreams", rulesProcessor.getOutputStreams());
        List<Rule> rulesWithWindow = new ArrayList<>();
        List<Rule> rulesWithoutWindow = new ArrayList<>();
        Set<String> inStreams = topologyDag.getEdgesTo(rulesProcessor)
                .stream()
                .flatMap(e -> e.getStreamGroupings()
                        .stream()
                        .map(sg -> sg.getStream().getId()))
                .collect(Collectors.toSet());

        for (Rule rule : rulesProcessor.getRules()) {
            if (!inStreams.containsAll(rule.getStreams())) {
                throw new IllegalStateException("Input streams of rules processor " + inStreams
                        + " does not contain rule's input streams " + rule.getStreams()
                        + ". Please delete and recreate the rule.");
            }
            if (rule.getWindow() != null) {
                rulesWithWindow.add(rule);
            } else {
                rulesWithoutWindow.add(rule);
            }
        }

        // assert that RulesProcessor doesn't have mixed kinds of rules.
        if (!rulesWithWindow.isEmpty() && !rulesWithoutWindow.isEmpty()) {
            throw new IllegalStateException("RulesProcessor should have either windowed or non-windowed rules, not both.");
        }
    }

    @Override
    public void visit(Edge edge) {
        if (sourceYamlComponentExists(edge) && !edgeAlreadyAddedComponents.contains(edge.getId())) {
            addEdge(edge.getFrom(),
                    edge.getTo());
            //Add the edges to the list of visited edges
            edgeAlreadyAddedComponents.add(edge.getId());

        }
    }

    private void getYamlComponents(BeamComponent fluxComponent, Component topologyComponent) {
        Map<String, Object> props = new LinkedHashMap<>();
        props.putAll(config);
        props.putAll(topologyComponent.getConfig().getProperties());
        props.put(BeamTopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY, topologyComponent);
        fluxComponent.withConfig(props, pipeline);
    }

    private boolean sourceYamlComponentExists(Edge edge) {
        if (visitedComponent.contains(getFluxId(edge.getFrom()))) {
            return true;
        }
        return false;
    }

    public List<Map.Entry<String, Component>> getYamlKeysAndComponents() {
        return keysAndComponents;
    }

    public Config getTopologyConfig() {
        return topologyConfig;
    }

  /* private BeamComponent getYamlComponents(BeamComponent beamComponent, Component topologyComponent)
   {
	  Map<String, Object> props = new LinkedHashMap<>();
	  props.putAll(config);
	  props.putAll(topologyComponent.getConfig().getProperties());
	  // below line is needed becuase kafka, normalization, notification and rules flux components need design time entities
	  props.put(BeamTopologyLayoutConstants.STREAMLINE_COMPONENT_CONF_KEY, topologyComponent);
	  beamComponent.withConfig(props, pipeline);

	  for (Map<String, Object> referencedComponent : beamComponent.getReferencedComponents())
	  {
		 keysAndComponents.add(makeEntry(BeamTopologyLayoutConstants.YAML_KEY_COMPONENTS, referencedComponent));
	  }

	  PTransform yamlComponent = beamComponent.getComponent();
	  yamlComponent.put(BeamTopologyLayoutConstants.YAML_KEY_ID, getFluxId(topologyComponent));
	  return yamlComponent;
   }*/

  /* private void addEdge(OutputComponent from, InputComponent to, String streamId, Stream.Grouping groupingType, List<String> fields)
   {
	  LinkFluxComponent fluxComponent = new LinkFluxComponent();
	  Map<String, Object> config = new HashMap<>();
	  Map<String, Object> grouping = new LinkedHashMap<>();
	  if (Stream.Grouping.FIELDS.equals(groupingType))
	  {
		 grouping.put(BeamTopologyLayoutConstants.YAML_KEY_TYPE, BeamTopologyLayoutConstants.YAML_KEY_CUSTOM_GROUPING);
		 Map<Object, Object> customGroupingClass = new HashMap<>();
		 customGroupingClass.put(BeamTopologyLayoutConstants.YAML_KEY_CLASS_NAME, BeamTopologyLayoutConstants.YAML_KEY_CUSTOM_GROUPING_CLASSNAME);
		 List<Object> constructorArgs = new ArrayList<>();
		 constructorArgs.add(fields);
		 customGroupingClass.put(BeamTopologyLayoutConstants.YAML_KEY_CONSTRUCTOR_ARGS, constructorArgs);
		 grouping.put(BeamTopologyLayoutConstants.YAML_KEY_CUSTOM_GROUPING_CLASS, customGroupingClass);
	  } else if (Stream.Grouping.SHUFFLE.equals(groupingType))
	  {
		 grouping.put(BeamTopologyLayoutConstants.YAML_KEY_TYPE, BeamTopologyLayoutConstants.YAML_KEY_LOCAL_OR_SHUFFLE_GROUPING);
	  } else
	  {
		 throw new RuntimeException("Unsupported grouping type: " + groupingType + "  for storm link ");
	  }
	  grouping.put(BeamTopologyLayoutConstants.YAML_KEY_STREAM_ID, streamId);
	  fluxComponent.updateLinkComponentWithGrouping(grouping);
	  config.put(BeamTopologyLayoutConstants.YAML_KEY_FROM, getFluxId(from));
	  config.put(BeamTopologyLayoutConstants.YAML_KEY_TO, getFluxId(to));
	  fluxComponent.withConfig(config);
	  Map<String, Object> yamlComponent = fluxComponent.getComponent();
	  keysAndComponents.add(makeEntry(BeamTopologyLayoutConstants.YAML_KEY_STREAMS, yamlComponent));
   }*/

    //
    private void addEdge(OutputComponent from, InputComponent to) {
        BeamComponent inputBeamComponent = componentMap.get(getFluxId(from));
        BeamComponent outputComponent = componentMap.get(getFluxId(to));

        PCollection<KV<String, String>> outputCollection = inputBeamComponent.getOutputCollection();

        if (outputComponent == null) {
            if (to instanceof StreamlineSink) {
                StreamlineSink sink = (StreamlineSink) to;
                visit(sink);
                BeamComponent beamSinkComponent = componentMap.get(getFluxId(sink));
                beamSinkComponent.generateComponent(outputCollection);
            } else if (to instanceof StreamlineProcessor) {
                StreamlineProcessor processor = (StreamlineProcessor) to;
                visit(processor);
                BeamComponent beamProcessorComponent = componentMap.get(getFluxId(processor));
                beamProcessorComponent.generateComponent(outputCollection);
            } else {
                throw new NotSupportedException("Unsupported Component Implementation");
            }


        }
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    private Map.Entry<String, Component> makeEntry(String key, Component component) {
        return new AbstractMap.SimpleImmutableEntry<>(key, component);
    }

    private String getFluxId(Component component) {
        return component.getId() + "-" + component.getName();
    }
}
