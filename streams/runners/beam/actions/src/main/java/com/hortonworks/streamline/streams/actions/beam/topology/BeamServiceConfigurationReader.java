package com.hortonworks.streamline.streams.actions.beam.topology;

import com.hortonworks.streamline.streams.actions.util.AutoCredsServiceConfigurationReader;
import com.hortonworks.streamline.streams.cluster.catalog.Namespace;
import com.hortonworks.streamline.streams.cluster.catalog.NamespaceServiceClusterMap;
import com.hortonworks.streamline.streams.cluster.catalog.Service;
import com.hortonworks.streamline.streams.cluster.service.EnvironmentService;
import java.util.Map;

/**
 * Created by Karthik.K
 */
public class BeamServiceConfigurationReader extends AutoCredsServiceConfigurationReader {

  public BeamServiceConfigurationReader(EnvironmentService environmentService, long namespaceId) {
    super(environmentService, namespaceId);
  }

  public Map<String, String> getRunnerConfig(Service beamRunnerService) {
    return super.read(beamRunnerService.getClusterId(), beamRunnerService.getName());
  }

  public String getStreamingEngine() {
    return environmentService.getNamespace(namespaceId).getStreamingEngine();
  }
}
