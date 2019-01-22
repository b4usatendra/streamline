package com.hortonworks.streamline.streams.actions.beam.topology;

import com.hortonworks.streamline.streams.actions.util.AutoCredsServiceConfigurationReader;
import com.hortonworks.streamline.streams.cluster.catalog.Namespace;
import com.hortonworks.streamline.streams.cluster.catalog.NamespaceServiceClusterMap;
import com.hortonworks.streamline.streams.cluster.service.EnvironmentService;
import java.util.Map;

/**
 * Created by Karthik.K
 */
public class BeamServiceConfigurationReader extends AutoCredsServiceConfigurationReader {

    private EnvironmentService environmentService;
    private long namespaceId;

    public BeamServiceConfigurationReader(EnvironmentService environmentService,long namespaceId){
        super(environmentService,namespaceId);
    }

    public Map<String,String> getRunnerConfig(){
        Namespace namespace = environmentService.getNamespace(namespaceId);
        NamespaceServiceClusterMap streamingEngine = (NamespaceServiceClusterMap) environmentService.listServiceClusterMapping(namespaceId).stream().filter((clusterMap)->clusterMap.getServiceName().equalsIgnoreCase(namespace.getStreamingEngine())).findAny().orElse(null);
        if(streamingEngine==null)
            return null;
        return super.read(streamingEngine.getClusterId(),streamingEngine.getServiceName());
    }

    public String getStreamingEngine(){
        return environmentService.getNamespace(namespaceId).getStreamingEngine();
    }
}
