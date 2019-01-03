package com.hortonworks.streamline.streams.actions.beam.topology;

import com.hortonworks.streamline.streams.actions.config.*;
import com.hortonworks.streamline.streams.actions.container.*;
import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.service.*;
import com.hortonworks.streamline.streams.layout.*;
import org.slf4j.*;

import javax.security.auth.*;
import java.util.*;

/**
 * @author satendra.sahu
 */
public class BeamTopologyActionsConfigImpl implements TopologyActionsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(BeamTopologyActionsConfigImpl.class);
    public static final String STREAMLINE_BEAM_JAR = "streamlineBeamJar";
    private static final String DEFAULT_BEAM_JAR_FILE_PREFIX = "streamline-runtime-beam-";

    private EnvironmentService environmentService;
    private TopologyActionsContainer topologyActionsContainer;
    private Map<String, String> streamlineConf;

    public Map<String, Object> buildConfig(TopologyActionsContainer topologyActionsContainer,
                                           Map<String, String> streamlineConf, Subject subject, Namespace namespace) {
        this.topologyActionsContainer = topologyActionsContainer;
        this.environmentService = topologyActionsContainer.getEnvironmentService();
        this.streamlineConf = streamlineConf;
        return buildBeamTopologyConfigMap(namespace, namespace.getStreamingEngine(), subject);
    }

    private Map<String, Object> buildBeamTopologyConfigMap(Namespace namespace, String streamingEngine, Subject subject) {

        // Assuming that a namespace has one mapping of streaming engine except test environment
        Service streamingEngineService = topologyActionsContainer.getFirstOccurenceServiceForNamespace(namespace, streamingEngine);
        if (streamingEngineService == null) {
            if (!namespace.getInternal()) {
                throw new RuntimeException("Streaming Engine " + streamingEngine + " is not associated to the namespace " +
                        namespace.getName() + "(" + namespace.getId() + ")");
            } else {
                // the namespace is purposed for test run
                return buildBeamTopologyActionsConfigMapForTestRun(namespace, subject);
            }
        }

        Map<String, Object> conf = new HashMap<>();

        // We need to have some local configurations anyway because topology submission can't be done with REST API.
        String beamJarLocation = streamlineConf.get(STREAMLINE_BEAM_JAR);
        if (beamJarLocation == null) {
            String jarFindDir = TopologyActionsContainer.applyReservedPaths(topologyActionsContainer.DEFAULT_JAR_LOCATION_DIR);
            beamJarLocation = TopologyActionsContainer.findFirstMatchingJarLocation(jarFindDir, DEFAULT_BEAM_JAR_FILE_PREFIX);
        } else {
            beamJarLocation = TopologyActionsContainer.applyReservedPaths(beamJarLocation);
        }


        conf.put(TopologyLayoutConstants.DEFAULT_ABSOLUTE_JAR_LOCATION_DIR, TopologyActionsContainer.applyReservedPaths(topologyActionsContainer.DEFAULT_JAR_LOCATION_DIR));
        // Since we're loading the class dynamically so we can't rely on any enums or constants from there
        conf.put(STREAMLINE_BEAM_JAR, beamJarLocation);
        conf.put(TopologyLayoutConstants.SUBJECT_OBJECT, subject);

        putBeamConfigurations(streamingEngineService, conf);

        // Topology during run-time will require few critical configs such as schemaRegistryUrl and catalogRootUrl
        // Hence its important to pass StreamlineConfig to TopologyConfig
        conf.putAll(streamlineConf);

        // TopologyActionImpl needs 'EnvironmentService' and namespace ID to load service configurations
        // for specific cluster associated to the namespace
        conf.put(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT, environmentService);
        conf.put(TopologyLayoutConstants.NAMESPACE_ID, namespace.getId());

        return conf;
    }

    private void putBeamConfigurations(Service streamingEngineService, Map<String, Object> conf) {

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
