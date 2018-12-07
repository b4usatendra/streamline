package com.hortonworks.streamline.streams.sampling.service.storm;

import com.hortonworks.streamline.streams.cluster.catalog.Component;
import com.hortonworks.streamline.streams.cluster.catalog.ComponentProcess;
import com.hortonworks.streamline.streams.cluster.catalog.Namespace;
import com.hortonworks.streamline.streams.cluster.catalog.Service;
import com.hortonworks.streamline.streams.cluster.discovery.ambari.ComponentPropertyPattern;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.sampling.service.TopologySamplingContainer;
import com.hortonworks.streamline.streams.sampling.service.config.TopologySamplingConfig;

import javax.security.auth.Subject;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
