/**
 * Copyright 2017 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.streamline.streams.actions.container;

import com.hortonworks.streamline.streams.actions.*;
import com.hortonworks.streamline.streams.actions.container.mapping.*;
import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.container.*;
import com.hortonworks.streamline.streams.cluster.discovery.ambari.*;
import com.hortonworks.streamline.streams.cluster.service.*;
import com.hortonworks.streamline.streams.layout.*;

import javax.security.auth.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

public class TopologyActionsContainer extends NamespaceAwareContainer<TopologyActions> {

    private static final String COMPONENT_NAME_STORM_UI_SERVER = ComponentPropertyPattern.STORM_UI_SERVER.name();
    private static final String COMPONENT_NAME_NIMBUS = ComponentPropertyPattern.NIMBUS.name();
    private static final String SERVICE_CONFIGURATION_STORM = ServiceConfigurations.STORM.getConfNames()[0];
    private static final String SERVICE_CONFIGURATION_STORM_ENV = ServiceConfigurations.STORM.getConfNames()[1];
    private static final String NIMBUS_SEEDS = "nimbus.seeds";
    private static final String NIMBUS_PORT = "nimbus.port";
    public static final String STREAMLINE_STORM_JAR = "streamlineStormJar";
    public static final String STREAMLINE_BEAM_JAR = "streamlineBeamJar";
    public static final String STORM_HOME_DIR = "stormHomeDir";
    public static final String RESERVED_PATH_STREAMLINE_HOME = "${STREAMLINE_HOME}";
    private static final String DEFAULT_JAR_LOCATION_DIR = "${STREAMLINE_HOME}/libs";
    public static final String SYSTEM_PROPERTY_STREAMLINE_HOME = "streamline.home";
    private static final String DEFAULT_JAR_FILE_PREFIX = "streamline-runtime";

    private final Map<String, String> streamlineConf;
    private final Subject subject;

    public TopologyActionsContainer(EnvironmentService environmentService, Map<String, String> streamlineConf,
                                    Subject subject) {
        super(environmentService);
        this.streamlineConf = streamlineConf;
        this.subject = subject;
    }

    @Override
    protected TopologyActions initializeInstance(Namespace namespace) {
        String streamingEngine = namespace.getStreamingEngine();
        MappedTopologyActionsImpl actionsImpl;
        // Only Storm is supported as streaming engine
        //TODO make streamEngine configurable
        try {
            actionsImpl = MappedTopologyActionsImpl.valueOf(streamingEngine);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Unsupported streaming engine: " + streamingEngine, e);
        }

        Map<String, Object> conf = null;
        //TODO Step-1 add beam topology action implementation
        // FIXME: "how to initialize" is up to implementation detail - now we just only consider about Storm implementation
        if (namespace.getStreamingEngine().equalsIgnoreCase(TopologyLayoutConstants.STORM_STREAMING_ENGINE.toLowerCase())) {
            conf = buildStormTopologyActionsConfigMap(namespace, streamingEngine, subject);
        } else if (namespace.getStreamingEngine().equalsIgnoreCase(TopologyLayoutConstants.BEAM_STREAMING_ENGINE.toLowerCase())) {
            conf = buildBeamTopologyConfigMap(namespace, streamingEngine, subject);
        }
        conf.put(TopologyLayoutConstants.DEFAULT_ABSOLUTE_JAR_LOCATION_DIR, applyReservedPaths(DEFAULT_JAR_LOCATION_DIR));
        String className = actionsImpl.getClassName();
        return initTopologyActions(conf, className);
    }

    private TopologyActions initTopologyActions(Map<String, Object> conf, String className) {
        try {
            TopologyActions topologyActions = instantiate(className);
            topologyActions.init(conf);
            return topologyActions;
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new RuntimeException("tialize Topology actions instance - Class Name: " + className, e);
        }
    }

    private Map<String, Object> buildBeamTopologyConfigMap(Namespace namespace, String streamingEngine, Subject subject) {

        // Assuming that a namespace has one mapping of streaming engine except test environment
        Service streamingEngineService = getFirstOccurenceServiceForNamespace(namespace, streamingEngine);
        if (streamingEngineService == null) {
            if (!namespace.getInternal()) {
                throw new RuntimeException("Streaming Engine " + streamingEngine + " is not associated to the namespace " +
                        namespace.getName() + "(" + namespace.getId() + ")");
            } else {
                // the namespace is purposed for test run
                return buildStormTopologyActionsConfigMapForTestRun(namespace, subject);
            }
        }

        Map<String, Object> conf = new HashMap<>();

        //adding config for beam-artifact location
        // We need to have some local configurations anyway because topology submission can't be done with REST API.
        String beamJarLocation = streamlineConf.get(STREAMLINE_BEAM_JAR);
        if (beamJarLocation == null) {
            String jarFindDir = applyReservedPaths(DEFAULT_JAR_LOCATION_DIR);
            beamJarLocation = findFirstMatchingJarLocation(streamingEngine, jarFindDir);
        } else {
            beamJarLocation = applyReservedPaths(beamJarLocation);
        }

        conf.put(STREAMLINE_BEAM_JAR, beamJarLocation);
        conf.put(STORM_HOME_DIR, streamlineConf.get(STORM_HOME_DIR));

        // Since we're loading the class dynamically so we can't rely on any enums or constants from there
        conf.put(TopologyLayoutConstants.SUBJECT_OBJECT, subject);

        //TODO runner configuration goes here
        //putStormConfigurations(streamingEngineService, conf);

        // Topology during run-time will require few critical configs such as schemaRegistryUrl and catalogRootUrl
        // Hence its important to pass StreamlineConfig to TopologyConfig
        conf.putAll(streamlineConf);

        // TopologyActionImpl needs 'EnvironmentService' and namespace ID to load service configurations
        // for specific cluster associated to the namespace
        conf.put(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT, environmentService);
        conf.put(TopologyLayoutConstants.NAMESPACE_ID, namespace.getId());

        return conf;
    }

    private Map<String, Object> buildStormTopologyActionsConfigMap(Namespace namespace, String streamingEngine, Subject subject) {
        // Assuming that a namespace has one mapping of streaming engine except test environment
        Service streamingEngineService = getFirstOccurenceServiceForNamespace(namespace, streamingEngine);
        if (streamingEngineService == null) {
            if (!namespace.getInternal()) {
                throw new RuntimeException("Streaming Engine " + streamingEngine + " is not associated to the namespace " +
                        namespace.getName() + "(" + namespace.getId() + ")");
            } else {
                // the namespace is purposed for test run
                return buildStormTopologyActionsConfigMapForTestRun(namespace, subject);
            }
        }

        Component uiServer = getComponent(streamingEngineService, COMPONENT_NAME_STORM_UI_SERVER)
                .orElseThrow(() -> new RuntimeException(streamingEngine + " doesn't have " + COMPONENT_NAME_STORM_UI_SERVER + " as component"));

        Collection<ComponentProcess> uiServerProcesses = environmentService.listComponentProcesses(uiServer.getId());
        if (uiServerProcesses.isEmpty()) {
            throw new RuntimeException(streamingEngine + " doesn't have component process " + COMPONENT_NAME_STORM_UI_SERVER);
        }

        ComponentProcess uiServerProcess = uiServerProcesses.iterator().next();
        final String uiHost = uiServerProcess.getHost();
        Integer uiPort = uiServerProcess.getPort();

        assertHostAndPort(uiServer.getName(), uiHost, uiPort);

        Component nimbus = getComponent(streamingEngineService, COMPONENT_NAME_NIMBUS)
                .orElseThrow(() -> new RuntimeException(streamingEngine + " doesn't have " + COMPONENT_NAME_NIMBUS + " as component"));

        Collection<ComponentProcess> nimbusProcesses = environmentService.listComponentProcesses(nimbus.getId());
        if (nimbusProcesses.isEmpty()) {
            throw new RuntimeException(streamingEngine + " doesn't have component process " + COMPONENT_NAME_NIMBUS);
        }

        List<String> nimbusHosts = nimbusProcesses.stream().map(ComponentProcess::getHost)
                .collect(Collectors.toList());
        Integer nimbusPort = nimbusProcesses.stream().map(ComponentProcess::getPort)
                .findAny().get();

        assertHostsAndPort(nimbus.getName(), nimbusHosts, nimbusPort);

        Map<String, Object> conf = new HashMap<>();

        // We need to have some local configurations anyway because topology submission can't be done with REST API.
        String stormJarLocation = streamlineConf.get(STREAMLINE_STORM_JAR);
        if (stormJarLocation == null) {
            String jarFindDir = applyReservedPaths(DEFAULT_JAR_LOCATION_DIR);
            stormJarLocation = findFirstMatchingJarLocation(streamingEngine, jarFindDir);
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
        // Hence its important to pass StreamlineConfig to TopologyConfig
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
            String jarFindDir = applyReservedPaths(DEFAULT_JAR_LOCATION_DIR);
            stormJarLocation = findFirstMatchingJarLocation(namespace.getStreamingEngine(), jarFindDir);
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
        // Hence its important to pass StreamlineConfig to TopologyConfig
        conf.putAll(streamlineConf);

        // TopologyActionImpl needs 'EnvironmentService' and namespace ID to load service configurations
        // for specific cluster associated to the namespace
        conf.put(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT, environmentService);
        conf.put(TopologyLayoutConstants.NAMESPACE_ID, namespace.getId());

        return conf;
    }

    private void putStormConfigurations(Service streamingEngineService, Map<String, Object> conf) {
        ServiceConfiguration storm = getServiceConfiguration(streamingEngineService, SERVICE_CONFIGURATION_STORM)
                .orElse(new ServiceConfiguration());
        ServiceConfiguration stormEnv = getServiceConfiguration(streamingEngineService, SERVICE_CONFIGURATION_STORM_ENV)
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

    private String findFirstMatchingJarLocation(String streamingEngine, String jarFindDir) {
        String[] jars = new File(jarFindDir).list((dir, name) -> {
            if (name.startsWith(DEFAULT_JAR_FILE_PREFIX + "-" + streamingEngine.toLowerCase() + "-") && name.endsWith(".jar")) {
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
        return stormJarLocation.replace(RESERVED_PATH_STREAMLINE_HOME, System.getProperty(SYSTEM_PROPERTY_STREAMLINE_HOME, getCWD()));
    }

    private String getCWD() {
        return Paths.get(".").toAbsolutePath().normalize().toString();
    }
}