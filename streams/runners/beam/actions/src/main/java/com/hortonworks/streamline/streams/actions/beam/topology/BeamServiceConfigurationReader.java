package com.hortonworks.streamline.streams.actions.beam.topology;

import com.hortonworks.streamline.streams.cluster.catalog.Namespace;
import com.hortonworks.streamline.streams.cluster.catalog.NamespaceServiceClusterMap;
import com.hortonworks.streamline.streams.cluster.catalog.ServiceConfiguration;
import com.hortonworks.streamline.streams.cluster.service.EnvironmentService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Karthik.K
 */
public class BeamServiceConfigurationReader{

    private EnvironmentService environmentService;
    private long namespaceId;

    enum BeamServiceConfigurations {
        BEAM("beam","beam-env");

        private final String[] confs;

        BeamServiceConfigurations(String... confs) {
            this.confs=confs;
        }
    }

    public BeamServiceConfigurationReader(EnvironmentService environmentService,long namespaceId){
        this.environmentService=environmentService;
        this.namespaceId=namespaceId;
    }

    public Map<String,String> getConfig(){
        Namespace namespace = environmentService.getNamespace(namespaceId);
        NamespaceServiceClusterMap streamingEngine = (NamespaceServiceClusterMap) environmentService.listServiceClusterMapping(namespaceId).stream().filter((clusterMap)->clusterMap.getServiceName().equalsIgnoreCase(namespace.getStreamingEngine())).findAny().orElse(null);
        if(streamingEngine==null)
            return null;
        long serviceId = environmentService.getServiceByName(streamingEngine.getClusterId(),streamingEngine.getServiceName()).getId();

        String[] confNames = BeamServiceConfigurations.valueOf(streamingEngine.getServiceName()).confs;
        Map<String, String> flattenConfig = new HashMap<>();

        Arrays.stream(confNames).forEach(conf->{
            try {
                ServiceConfiguration beamConf = environmentService.getServiceConfigurationByName(serviceId,conf);
                if(beamConf!=null)
                    flattenConfig.putAll(beamConf.getConfigurationMap());
            } catch (IOException e) {
                throw new RuntimeException(String.format("Can't read configuration from serviceId: %d conf: %d",serviceId,conf));
            }
        });
        return flattenConfig;
    }

    public String getStreamingEngine(){
        return environmentService.getNamespace(namespaceId).getStreamingEngine();
    }
}
