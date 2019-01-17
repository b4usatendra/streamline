package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.streams.layout.component.TopologyLayout;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by Satendra Sahu on 12/6/18
 */
public class TopologyMapper implements Serializable {

  private Map<String, Object> conf;
  private TopologyLayout topologyLayout;

  public TopologyMapper(Map<String, Object> conf, TopologyLayout topologyLayout) {
    this.conf = conf;
    this.topologyLayout = topologyLayout;
  }

  public Map<String, Object> getConf() {
    return conf;
  }

  public TopologyLayout getTopologyLayout() {
    return topologyLayout;
  }
}
