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
import com.hortonworks.streamline.streams.actions.config.mapping.*;
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


import javax.security.auth.*;
import java.util.*;


public class TopologyActionsContainer extends NamespaceAwareContainer<TopologyActions> {

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

        try {
            actionsImpl = MappedTopologyActionsImpl.valueOf(streamingEngine);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Unsupported streaming engine: " + streamingEngine, e);
        }


        // FIXME: "how to initialize" is up to implementation detail - now we just only consider about Storm implementation
//        Map<String, Object> conf = buildStormTopologyActionsConfigMap(namespace, streamingEngine, subject);

        ConfigAwareContainer topologyActionsConfig = initActionsConfig(streamingEngine);
        final Map<String, Object> conf = topologyActionsConfig.buildConfig(this, streamlineConf, subject, namespace);

        String className = actionsImpl.getClassName();
        return initTopologyActions(conf, className);
    }


    private ConfigAwareContainer initActionsConfig(String streamingEngine) {
        MappedTopologyActionsConfigImpl actionsConfigImpl;
        try {
            actionsConfigImpl = MappedTopologyActionsConfigImpl.valueOf(streamingEngine);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Unsupported streaming engine: " + streamingEngine, e);
        }
        String className = actionsConfigImpl.getClassName();
        try {
            Class<ConfigAwareContainer> clazz = (Class<ConfigAwareContainer>) Class.forName(className);
            return clazz.newInstance();
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new RuntimeException("Can't initialize Topology actions config instance - Class Name: " + className, e);
        }
    }

    private TopologyActions initTopologyActions(Map<String, Object> conf, String className) {
        try {
            TopologyActions topologyActions = instantiate(className);
            topologyActions.init(conf);
            return topologyActions;
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new RuntimeException("Can't initialize Topology actions instance - Class Name: " + className, e);
        }
    }
}