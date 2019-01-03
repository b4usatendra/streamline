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
package com.hortonworks.streamline.streams.actions.container;


import com.hortonworks.streamline.streams.actions.*;
import com.hortonworks.streamline.streams.actions.config.mapping.*;
import com.hortonworks.streamline.streams.actions.container.mapping.*;
import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.container.*;
import com.hortonworks.streamline.streams.cluster.service.*;

import javax.security.auth.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;


public class TopologyActionsContainer extends NamespaceAwareContainer<TopologyActions> {

    public static final String RESERVED_PATH_STREAMLINE_HOME = "${STREAMLINE_HOME}";
    public static final String SYSTEM_PROPERTY_STREAMLINE_HOME = "streamline.home";
    public static final String DEFAULT_JAR_LOCATION_DIR = "${STREAMLINE_HOME}/libs";

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

    public static String applyReservedPaths(String steamingEngineJarLocation) {
        return steamingEngineJarLocation.replace(RESERVED_PATH_STREAMLINE_HOME, System.getProperty(SYSTEM_PROPERTY_STREAMLINE_HOME, getCWD()));
    }

    public static String getCWD() {
        return Paths.get(".").toAbsolutePath().normalize().toString();
    }

    public static String findFirstMatchingJarLocation(String jarFindDir, String jarNamePrefix) {
        String[] jars = new File(jarFindDir).list((dir, name) -> {
            if (name.startsWith(jarNamePrefix) && name.endsWith(".jar")) {
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
}