package com.hortonworks.streamline.streams.actions.beam.topology;

import com.hortonworks.streamline.streams.actions.config.*;
import com.hortonworks.streamline.streams.actions.container.*;
import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.discovery.ambari.*;
import com.hortonworks.streamline.streams.cluster.service.*;
import com.hortonworks.streamline.streams.layout.*;
import org.slf4j.*;

import javax.security.auth.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * @author suman.bn
 */
public class BeamTopologyActionsConfigImpl implements TopologyActionsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(BeamTopologyActionsConfigImpl.class);

    private static final String COMPONENT_NAME_STORM_UI_SERVER = ComponentPropertyPattern.STORM_UI_SERVER.name();
    private static final String COMPONENT_NAME_NIMBUS = ComponentPropertyPattern.NIMBUS.name();
    private static final String SERVICE_CONFIGURATION_STORM = ServiceConfigurations.STORM.getConfNames()[0];
    private static final String SERVICE_CONFIGURATION_STORM_ENV = ServiceConfigurations.STORM.getConfNames()[1];

    private static final String NIMBUS_SEEDS = "nimbus.seeds";
    private static final String NIMBUS_PORT = "nimbus.port";
    public static final String STREAMLINE_STORM_JAR = "streamlineStormJar";
    public static final String STORM_HOME_DIR = "stormHomeDir";

    public static final String RESERVED_PATH_STREAMLINE_HOME = "${STREAMLINE_HOME}";
    public static final String SYSTEM_PROPERTY_STREAMLINE_HOME = "streamline.home";
    private static final String DEFAULT_STORM_JAR_LOCATION_DIR = "${STREAMLINE_HOME}/libs";
    private static final String DEFAULT_STORM_JAR_FILE_PREFIX = "streamline-runtime-storm-";

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
        String stormJarLocation = streamlineConf.get(STREAMLINE_STORM_JAR);
        if (stormJarLocation == null) {
            String jarFindDir = applyReservedPaths(DEFAULT_STORM_JAR_LOCATION_DIR);
            stormJarLocation = findFirstMatchingJarLocation(jarFindDir);
        } else {
            stormJarLocation = applyReservedPaths(stormJarLocation);
        }


        conf.put(STORM_HOME_DIR, streamlineConf.get(STORM_HOME_DIR));

        // Since we're loading the class dynamically so we can't rely on any enums or constants from there

        conf.put(TopologyLayoutConstants.SUBJECT_OBJECT, subject);

        putStormConfigurations(streamingEngineService, conf);

        // Topology during run-time will require few critical configs such as schemaRegistryUrl and catalogRootUrl
        // Hence its important to pass StreamlineConfig to TopologyConfig
        conf.putAll(streamlineConf);

        // TopologyActionImpl needs 'EnvironmentService' and namespace ID to load service configurations
        // for specific cluster associated to the namespace
        conf.put(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT, environmentService);
        conf.put(TopologyLayoutConstants.NAMESPACE_ID, namespace.getId());

        return conf;
    }

    private void putStormConfigurations(Service streamingEngineService, Map<String, Object> conf) {
        ServiceConfiguration storm = topologyActionsContainer.getServiceConfiguration(streamingEngineService, SERVICE_CONFIGURATION_STORM)
                .orElse(new ServiceConfiguration());
        ServiceConfiguration stormEnv = topologyActionsContainer.getServiceConfiguration(streamingEngineService, SERVICE_CONFIGURATION_STORM_ENV)
                .orElse(new ServiceConfiguration());

        try {
            Map<String, String> stormConfMap = storm.getConfigurationMap();
            if (stormConfMap != null) {
                if (stormConfMap.containsKey(TopologyLayoutConstants.NIMBUS_THRIFT_MAX_BUFFER_SIZE)) {
                    conf.put(TopologyLayoutConstants.NIMBUS_THRIFT_MAX_BUFFER_SIZE,
                            Long.parseLong(stormConfMap.get(TopologyLayoutConstants.NIMBUS_THRIFT_MAX_BUFFER_SIZE)));
                }

                if (stormConfMap.containsKey(TopologyLayoutConstants.STORM_THRIFT_TRANSPORT)) {
                    String thriftTransport = stormConfMap.get(TopologyLayoutConstants.STORM_THRIFT_TRANSPORT);
                    if (!thriftTransport.startsWith("{{") && !thriftTransport.endsWith("}}")) {
                        conf.put(TopologyLayoutConstants.STORM_THRIFT_TRANSPORT, stormConfMap.get(TopologyLayoutConstants.STORM_THRIFT_TRANSPORT));
                    }
                }

                if (stormConfMap.containsKey(TopologyLayoutConstants.STORM_NONSECURED_THRIFT_TRANSPORT)) {
                    conf.put(TopologyLayoutConstants.STORM_NONSECURED_THRIFT_TRANSPORT, stormConfMap.get(TopologyLayoutConstants.STORM_NONSECURED_THRIFT_TRANSPORT));
                }

                if (stormConfMap.containsKey(TopologyLayoutConstants.STORM_SECURED_THRIFT_TRANSPORT)) {
                    conf.put(TopologyLayoutConstants.STORM_SECURED_THRIFT_TRANSPORT, stormConfMap.get(TopologyLayoutConstants.STORM_SECURED_THRIFT_TRANSPORT));
                }

                if (stormConfMap.containsKey(TopologyLayoutConstants.STORM_PRINCIPAL_TO_LOCAL)) {
                    conf.put(TopologyLayoutConstants.STORM_PRINCIPAL_TO_LOCAL, stormConfMap.get(TopologyLayoutConstants.STORM_PRINCIPAL_TO_LOCAL));
                }
            }

            Map<String, String> stormEnvConfMap = stormEnv.getConfigurationMap();
            if (stormEnvConfMap != null) {
                if (stormEnvConfMap.containsKey(TopologyLayoutConstants.STORM_NIMBUS_PRINCIPAL_NAME)) {
                    conf.put(TopologyLayoutConstants.STORM_NIMBUS_PRINCIPAL_NAME, stormEnvConfMap.get(TopologyLayoutConstants.STORM_NIMBUS_PRINCIPAL_NAME));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String findFirstMatchingJarLocation(String jarFindDir) {
        String[] jars = new File(jarFindDir).list((dir, name) -> {
            if (name.startsWith(DEFAULT_STORM_JAR_FILE_PREFIX) && name.endsWith(".jar")) {
                return true;
            }
            return false;
        });

        if (jars == null || jars.length == 0) {
            return null;
        } else {
            return jarFindDir + File.separator + jars[0];
        }
    }


    private Map<String, Object> buildBeamTopologyActionsConfigMapForTestRun(Namespace namespace, Subject subject) {
        Map<String, Object> conf = new HashMap<>();

        // We need to have some local configurations anyway because topology submission can't be done with REST API.
        String stormJarLocation = streamlineConf.get(STREAMLINE_STORM_JAR);
        if (stormJarLocation == null) {
            String jarFindDir = applyReservedPaths(DEFAULT_STORM_JAR_LOCATION_DIR);
            stormJarLocation = findFirstMatchingJarLocation(jarFindDir);
        } else {
            stormJarLocation = applyReservedPaths(stormJarLocation);
        }

        conf.put(STREAMLINE_STORM_JAR, stormJarLocation);
        conf.put(STORM_HOME_DIR, streamlineConf.get(STORM_HOME_DIR));

        // Since we're loading the class dynamically so we can't rely on any enums or constants from there
        // belows are all dummy value which is not used for test topology run
        conf.put(NIMBUS_SEEDS, "localhost");
        conf.put(NIMBUS_PORT, "6627");
        conf.put(TopologyLayoutConstants.STORM_API_ROOT_URL_KEY, "http://localhost:8080");
        conf.put(TopologyLayoutConstants.SUBJECT_OBJECT, subject);

        // Topology during run-time will require few critical configs such as schemaRegistryUrl and catalogRootUrl
        // Hence its important to pass StreamlineConfig to TopologyConfigA
        conf.putAll(streamlineConf);

        // TopologyActionImpl needs 'EnvironmentService' and namespace ID to load service configurations
        // for specific cluster associated to the namespace
        conf.put(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT, environmentService);
        conf.put(TopologyLayoutConstants.NAMESPACE_ID, namespace.getId());

        return conf;
    }

    private String applyReservedPaths(String stormJarLocation) {
        return stormJarLocation.replace(RESERVED_PATH_STREAMLINE_HOME, System.getProperty(SYSTEM_PROPERTY_STREAMLINE_HOME, getCWD()));
    }

    private String getCWD() {
        return Paths.get(".").toAbsolutePath().normalize().toString();
    }
}
