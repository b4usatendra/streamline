package com.hortonworks.streamline.streams.actions.config;

import com.hortonworks.streamline.streams.actions.container.TopologyActionsContainer;
import com.hortonworks.streamline.streams.cluster.catalog.Namespace;
import com.hortonworks.streamline.streams.cluster.container.ConfigAwareContainer;
import com.hortonworks.streamline.streams.cluster.service.EnvironmentService;

import javax.security.auth.Subject;
import java.util.Map;

/**
 * @author suman.bn
 */
public abstract class TopologyActionsConfig implements ConfigAwareContainer<TopologyActionsContainer> {

  protected EnvironmentService environmentService;
  protected TopologyActionsContainer topologyActionsContainer;
  protected Map<String, String> streamlineConf;


  public Map<String, Object> buildConfig(TopologyActionsContainer topologyActionsContainer,
                                         Map<String, String> streamlineConf, Subject subject, Namespace namespace) {
    this.topologyActionsContainer = topologyActionsContainer;
    this.environmentService = topologyActionsContainer.getEnvironmentService();
    this.streamlineConf = streamlineConf;
    return buildTopologyConfigMap(namespace, namespace.getStreamingEngine(), subject);
  }

  protected abstract Map<String, Object> buildTopologyConfigMap(Namespace namespace, String streamingEngine, Subject subject);
}
