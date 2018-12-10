package com.hortonworks.streamline.streams.sampling.service.storm;

import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.discovery.ambari.*;
import com.hortonworks.streamline.streams.layout.*;
import com.hortonworks.streamline.streams.sampling.service.*;
import com.hortonworks.streamline.streams.sampling.service.config.*;

import javax.security.auth.*;
import java.util.*;

/**
 * @author suman.bn
 */
public class StormTopologySamplingConfigImpl implements TopologySamplingConfig {

    public static final String COMPONENT_NAME_STORM_UI_SERVER = ComponentPropertyPattern.STORM_UI_SERVER.name();

    @Override
    public Map<String, Object> buildConfig(TopologySamplingContainer topologySamplingContainer, Map<String, String> streamlineConf, Subject subject, Namespace namespace) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(TopologyLayoutConstants.STORM_API_ROOT_URL_KEY, buildStormRestApiRootUrl(namespace, topologySamplingContainer));
        conf.put(TopologyLayoutConstants.SUBJECT_OBJECT, subject);
        return conf;
    }

    private String buildStormRestApiRootUrl(Namespace namespace, TopologySamplingContainer topologySamplingContainer) {
        // Assuming that a namespace has one mapping of streaming engine
        String streamingEngine = namespace.getStreamingEngine();
        Service streamingEngineService = topologySamplingContainer.getFirstOccurenceServiceForNamespace(namespace, streamingEngine);
        if (streamingEngineService == null) {
            throw new RuntimeException("Streaming Engine " + streamingEngine + " is not associated to the namespace " +
                    namespace.getName() + "(" + namespace.getId() + ")");
        }
        Component uiServer = topologySamplingContainer.getComponent(streamingEngineService, COMPONENT_NAME_STORM_UI_SERVER)
                .orElseThrow(() -> new RuntimeException(streamingEngine + " doesn't have " + COMPONENT_NAME_STORM_UI_SERVER + " as component"));
        Collection<ComponentProcess> uiServerProcesses = topologySamplingContainer.getEnvironmentService().listComponentProcesses(uiServer.getId());
        if (uiServerProcesses.isEmpty()) {
            throw new RuntimeException(streamingEngine + " doesn't have any process for " + COMPONENT_NAME_STORM_UI_SERVER + " as component");
        }
        ComponentProcess uiServerProcess = uiServerProcesses.iterator().next();
        String uiHost = uiServerProcess.getHost();
        Integer uiPort = uiServerProcess.getPort();
        topologySamplingContainer.assertHostAndPort(uiServer.getName(), uiHost, uiPort);
        return "http://" + uiHost + ":" + uiPort + "/api/v1";
    }
}
