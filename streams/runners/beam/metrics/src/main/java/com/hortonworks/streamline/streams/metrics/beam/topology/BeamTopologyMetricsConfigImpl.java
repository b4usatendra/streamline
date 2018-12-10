package com.hortonworks.streamline.streams.metrics.beam.topology;

import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.discovery.ambari.*;
import com.hortonworks.streamline.streams.layout.*;
import com.hortonworks.streamline.streams.metrics.config.*;
import com.hortonworks.streamline.streams.metrics.container.*;

import javax.security.auth.*;
import java.util.*;

/**
 * @author suman.bn
 */
public class BeamTopologyMetricsConfigImpl implements TopologyMetricsConfig {

    public static final String COMPONENT_NAME_STORM_UI_SERVER = ComponentPropertyPattern.STORM_UI_SERVER.name();

    @Override
    public Map<String, Object> buildConfig(TopologyMetricsContainer topologyMetricsContainer, Map<String, String> streamlineConf, Subject subject, Namespace namespace) {
        Map<String, Object> conf = new HashMap<>();
        return conf;
    }
}
