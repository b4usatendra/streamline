package com.hortonworks.streamline.streams.actions.beam.topology;

import com.hortonworks.streamline.streams.actions.config.TopologyActionsConfig;
import com.hortonworks.streamline.streams.actions.container.TopologyActionsContainer;
import com.hortonworks.streamline.streams.beam.common.BeamRunner;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.cluster.catalog.Namespace;
import com.hortonworks.streamline.streams.cluster.catalog.Service;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author satendra.sahu
 */
public class BeamTopologyActionsConfigImpl extends TopologyActionsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(BeamTopologyActionsConfigImpl.class);
    private static final String STREAMLINE_BEAM_JAR = "streamlineBeamJar";
    private static final String DEFAULT_BEAM_JAR_FILE_PREFIX = "streamline-runtime-beam-";
    private Service beamRunnerService;

    public Map<String, Object> buildConfig(TopologyActionsContainer topologyActionsContainer,
        Map<String, String> streamlineConf, Subject subject, Namespace namespace) {
        return super.buildConfig(topologyActionsContainer, streamlineConf, subject, namespace);
    }

    //TODO: add beam/flink configurations
    protected Map<String, Object> buildTopologyConfigMap(Namespace namespace, String streamingEngine, Subject subject) {

        //Beam pipeline can have many runners associated with it.

        for (BeamRunner runner : BeamRunner.values()) {
            beamRunnerService = topologyActionsContainer.getFirstOccurenceServiceForNamespace(namespace, runner.name());
            if (beamRunnerService != null) {
                break;
            }
        }

        if (beamRunnerService == null) {
            if (!namespace.getInternal()) {
                throw new RuntimeException("Streaming Engine " + streamingEngine + " is not associated to the namespace " +
                    namespace.getName() + "(" + namespace.getId() + ")");
            } else {
                // the namespace is purposed for test run
                return buildBeamTopologyActionsConfigMapForTestRun(namespace, subject);
            }
        }

        Map<String, Object> conf = new HashMap<>();
        conf.put(TopologyLayoutConstants.BEAM_RUNNER_SERVICE, beamRunnerService);

        // We need to have some local configurations anyway because topology submission can't be done with REST API.
        String beamJarLocation = streamlineConf.get(STREAMLINE_BEAM_JAR);
        if (beamJarLocation == null) {
            String jarFindDir = TopologyActionsContainer.applyReservedPaths(topologyActionsContainer.DEFAULT_JAR_LOCATION_DIR);
            beamJarLocation = TopologyActionsContainer.findFirstMatchingJarLocation(jarFindDir, DEFAULT_BEAM_JAR_FILE_PREFIX);
        } else {
            beamJarLocation = TopologyActionsContainer.applyReservedPaths(beamJarLocation);
        }

        conf.put(TopologyLayoutConstants.DEFAULT_ABSOLUTE_JAR_LOCATION_DIR, TopologyActionsContainer.applyReservedPaths(topologyActionsContainer.DEFAULT_JAR_LOCATION_DIR));
        conf.put(STREAMLINE_BEAM_JAR, beamJarLocation);
        conf.put(TopologyLayoutConstants.SUBJECT_OBJECT, subject);

        // Topology during run-time will require few critical configs such as schemaRegistryUrl and catalogRootUrl
        // Hence its important to pass StreamlineConfig to TopologyConfig
        conf.putAll(streamlineConf);

        putFlinkConfigurations(conf);
        // TopologyActionImpl needs 'EnvironmentService' and namespace ID to load service configurations
        // for specific cluster associated to the namespace
        conf.put(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT, environmentService);
        conf.put(TopologyLayoutConstants.NAMESPACE_ID, namespace.getId());

        return conf;
    }

    private Map<String, Object> putFlinkConfigurations(Map<String, Object> conf) {
        return conf;
    }

    private Map<String, Object> buildBeamTopologyActionsConfigMapForTestRun(Namespace namespace, Subject subject) {
        Map<String, Object> conf = new HashMap<>();

        // We need to have some local configurations anyway because topology submission can't be done with REST API.
        String beamJarLocation = streamlineConf.get(STREAMLINE_BEAM_JAR);
        if (beamJarLocation == null) {
            String jarFindDir = TopologyActionsContainer.applyReservedPaths(topologyActionsContainer.DEFAULT_JAR_LOCATION_DIR);
            beamJarLocation = TopologyActionsContainer.findFirstMatchingJarLocation(jarFindDir, DEFAULT_BEAM_JAR_FILE_PREFIX);
        } else {
            beamJarLocation = TopologyActionsContainer.applyReservedPaths(beamJarLocation);
        }
        PipelineOptions options = PipelineOptionsFactory.create();
        conf.put(BeamTopologyLayoutConstants.BEAM_PIPELINE_OPTIONS, options);
        conf.put(STREAMLINE_BEAM_JAR, beamJarLocation);
        // Topology during run-time will require few critical configs such as schemaRegistryUrl and catalogRootUrl
        // Hence its important to pass StreamlineConfig to TopologyConfigA
        conf.putAll(streamlineConf);

        // TopologyActionImpl needs 'EnvironmentService' and namespace ID to load service configurations
        // for specific cluster associated to the namespace
        conf.put(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT, environmentService);
        conf.put(TopologyLayoutConstants.NAMESPACE_ID, namespace.getId());

        return conf;
    }


}
