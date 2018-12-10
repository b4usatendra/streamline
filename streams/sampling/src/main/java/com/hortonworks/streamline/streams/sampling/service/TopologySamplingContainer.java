package com.hortonworks.streamline.streams.sampling.service;

import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.container.*;
import com.hortonworks.streamline.streams.cluster.service.*;
import com.hortonworks.streamline.streams.sampling.service.config.mapping.*;
import com.hortonworks.streamline.streams.sampling.service.mapping.*;

import javax.security.auth.*;
import java.util.*;

public class TopologySamplingContainer extends NamespaceAwareContainer<TopologySampling> {
    private final Subject subject;

    public TopologySamplingContainer(EnvironmentService environmentService, Subject subject) {
        super(environmentService);
        this.subject = subject;
    }

    @Override
    protected TopologySampling initializeInstance(Namespace namespace) {
        String streamingEngine = namespace.getStreamingEngine();

        MappedTopologySamplingImpl samplingImpl;
        try {
            samplingImpl = MappedTopologySamplingImpl.valueOf(streamingEngine);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Unsupported streaming engine: " + streamingEngine, e);
        }

        ConfigAwareContainer topologySamplingConfig = initSamplingConfig(streamingEngine);
        final Map<String, Object> conf = topologySamplingConfig.buildConfig(this, null, subject, namespace);

        String className = samplingImpl.getClassName();
        return initTopologySampling(conf, className);
    }


    private TopologySampling initTopologySampling(Map<String, Object> conf, String className) {
        try {
            TopologySampling topologySampling = instantiate(className);
            topologySampling.init(conf);
            return topologySampling;
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new RuntimeException("Can't initialize Topology actions instance - Class Name: " + className, e);
        }
    }


      private ConfigAwareContainer initSamplingConfig(String streamingEngine) {
        MappedTopologySamplingConfigImpl actionsConfigImpl;
        try {
            actionsConfigImpl = MappedTopologySamplingConfigImpl.valueOf(streamingEngine);
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

//    private Map<String, Object> buildConfig(Namespace namespace, String streamingEngine) {
//        Map<String, Object> conf = new HashMap<>();
//        conf.put(TopologyLayoutConstants.STORM_API_ROOT_URL_KEY, buildStormRestApiRootUrl(namespace, streamingEngine));
//        conf.put(TopologyLayoutConstants.SUBJECT_OBJECT, subject);
//        return conf;
//    }

}
