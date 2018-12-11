package com.hortonworks.streamline.streams.actions.storm.topology;

import com.hortonworks.streamline.streams.actions.config.TopologyActionsConfig;
import com.hortonworks.streamline.streams.actions.container.TopologyActionsContainer;
import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.discovery.ambari.ComponentPropertyPattern;
import com.hortonworks.streamline.streams.cluster.discovery.ambari.ServiceConfigurations;
import com.hortonworks.streamline.streams.cluster.service.EnvironmentService;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author suman.bn
 */
public class StormTopologyActionsConfigImpl implements TopologyActionsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(StormTopologyActionsConfigImpl.class);

    private static final String COMPONENT_NAME_STORM_UI_SERVER = ComponentPropertyPattern.STORM_UI_SERVER.name();
    private static final String COMPONENT_NAME_NIMBUS = ComponentPropertyPattern.NIMBUS.name();
    private static final String SERVICE_CONFIGURATION_STORM = ServiceConfigurations.STORM.getConfNames()[0];
    private static final String SERVICE_CONFIGURATION_STORM_ENV = ServiceConfigurations.STORM.getConfNames()[1];

    private static final String NIMBUS_SEEDS = "nimbus.seeds";
    private static final String NIMBUS_PORT = "nimbus.port";
    public static final String STREAMLINE_STORM_JAR = "streamlineStormJar";
    public static final String STORM_HOME_DIR = "stormHomeDir";
    private static final String DEFAULT_STORM_JAR_FILE_PREFIX = "streamline-runtime-storm-";

    private EnvironmentService environmentService;
    private TopologyActionsContainer topologyActionsContainer;
    private Map<String, String> streamlineConf;

    public Map<String, Object> buildConfig(TopologyActionsContainer topologyActionsContainer,
                                           Map<String, String> streamlineConf, Subject subject, Namespace namespace) {
        this.topologyActionsContainer = topologyActionsContainer;
        this.environmentService = topologyActionsContainer.getEnvironmentService();
        this.streamlineConf = streamlineConf;
        return buildStormTopologyActionsConfigMap(namespace, subject);
    }

    private Map<String, Object> buildStormTopologyActionsConfigMap(Namespace namespace, Subject subject) {
        // Assuming that a namespace has one mapping of streaming engine except test environment
        String streamingEngine = namespace.getStreamingEngine();
        Service streamingEngineService = topologyActionsContainer.getFirstOccurenceServiceForNamespace(namespace, streamingEngine);
        if (streamingEngineService == null) {
            if (!namespace.getInternal()) {
                throw new RuntimeException("Streaming Engine " + streamingEngine + " is not associated to the namespace " +
                        namespace.getName() + "(" + namespace.getId() + ")");
            } else {
                // the namespace is purposed for test run
                return buildStormTopologyActionsConfigMapForTestRun(namespace, subject);
            }
        }

        Component uiServer = topologyActionsContainer.getComponent(streamingEngineService, COMPONENT_NAME_STORM_UI_SERVER)
                .orElseThrow(() -> new RuntimeException(streamingEngine + " doesn't have " + COMPONENT_NAME_STORM_UI_SERVER + " as component"));

        Collection<ComponentProcess> uiServerProcesses = environmentService.listComponentProcesses(uiServer.getId());
        if (uiServerProcesses.isEmpty()) {
            throw new RuntimeException(streamingEngine + " doesn't have component process " + COMPONENT_NAME_STORM_UI_SERVER);
        }

        ComponentProcess uiServerProcess = uiServerProcesses.iterator().next();
        final String uiHost = uiServerProcess.getHost();
        Integer uiPort = uiServerProcess.getPort();

        topologyActionsContainer.assertHostAndPort(uiServer.getName(), uiHost, uiPort);

        Component nimbus = topologyActionsContainer.getComponent(streamingEngineService, COMPONENT_NAME_NIMBUS)
                .orElseThrow(() -> new RuntimeException(streamingEngine + " doesn't have " + COMPONENT_NAME_NIMBUS + " as component"));

        Collection<ComponentProcess> nimbusProcesses = environmentService.listComponentProcesses(nimbus.getId());
        if (nimbusProcesses.isEmpty()) {
            throw new RuntimeException(streamingEngine + " doesn't have component process " + COMPONENT_NAME_NIMBUS);
        }

        List<String> nimbusHosts = nimbusProcesses.stream().map(ComponentProcess::getHost)
                .collect(Collectors.toList());
        Integer nimbusPort = nimbusProcesses.stream().map(ComponentProcess::getPort)
                .findAny().get();

        topologyActionsContainer.assertHostsAndPort(nimbus.getName(), nimbusHosts, nimbusPort);

        Map<String, Object> conf = new HashMap<>();

        // We need to have some local configurations anyway because topology submission can't be done with REST API.
        String stormJarLocation = streamlineConf.get(STREAMLINE_STORM_JAR);
        if (stormJarLocation == null) {
            String jarFindDir = applyReservedPaths(topologyActionsContainer.DEFAULT_JAR_LOCATION_DIR);
            stormJarLocation = findFirstMatchingJarLocation(jarFindDir);
        } else {
            stormJarLocation = applyReservedPaths(stormJarLocation);
        }

        conf.put(STREAMLINE_STORM_JAR, stormJarLocation);
        conf.put(STORM_HOME_DIR, streamlineConf.get(STORM_HOME_DIR));

        // Since we're loading the class dynamically so we can't rely on any enums or constants from there
        conf.put(NIMBUS_SEEDS, String.join(",", nimbusHosts));
        conf.put(NIMBUS_PORT, String.valueOf(nimbusPort));
        conf.put(TopologyLayoutConstants.STORM_API_ROOT_URL_KEY, buildStormRestApiRootUrl(uiHost, uiPort));
        conf.put(TopologyLayoutConstants.SUBJECT_OBJECT, subject);

        putStormConfigurations(streamingEngineService, conf);

        // Topology during run-time will require few critical configs such as schemaRegistryUrl and catalogRootUrl
        // Hence its important to pass StreamlineConfig to TopologyConfigA
        conf.putAll(streamlineConf);

        // TopologyActionImpl needs 'EnvironmentService' and namespace ID to load service configurations
        // for specific cluster associated to the namespace
        conf.put(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT, environmentService);
        conf.put(TopologyLayoutConstants.NAMESPACE_ID, namespace.getId());

        return conf;
    }

    private Map<String, Object> buildStormTopologyActionsConfigMapForTestRun(Namespace namespace, Subject subject) {
        Map<String, Object> conf = new HashMap<>();

        // We need to have some local configurations anyway because topology submission can't be done with REST API.
        String stormJarLocation = streamlineConf.get(STREAMLINE_STORM_JAR);
        if (stormJarLocation == null) {
            String jarFindDir = applyReservedPaths(topologyActionsContainer.DEFAULT_JAR_LOCATION_DIR);
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

    private String buildStormRestApiRootUrl(String host, Integer port) {
        return "http://" + host + ":" + port + "/api/v1";
    }

    private String applyReservedPaths(String stormJarLocation) {
        return stormJarLocation.replace(topologyActionsContainer.RESERVED_PATH_STREAMLINE_HOME, System.getProperty(topologyActionsContainer.SYSTEM_PROPERTY_STREAMLINE_HOME, getCWD()));
    }

    private String getCWD() {
        return Paths.get(".").toAbsolutePath().normalize().toString();
    }
}
