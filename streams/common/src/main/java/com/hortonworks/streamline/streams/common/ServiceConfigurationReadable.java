package com.hortonworks.streamline.streams.common;

import java.util.Map;

public interface ServiceConfigurationReadable {
    Map<Long, Map<String, String>> readAllClusters(String serviceName);
    Map<String, String> read(Long clusterId, String serviceName);
}
