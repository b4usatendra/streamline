/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.streamline.streams.cluster.register.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.hortonworks.streamline.common.Config;
import com.hortonworks.streamline.streams.cluster.Constants;
import com.hortonworks.streamline.streams.cluster.catalog.Component;
import com.hortonworks.streamline.streams.cluster.catalog.ComponentProcess;
import com.hortonworks.streamline.streams.cluster.catalog.ServiceConfiguration;
import com.hortonworks.streamline.streams.cluster.discovery.ambari.ServiceConfigurations;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkServiceRegistrar extends AbstractServiceRegistrar {

  private static final String FLINK = ServiceConfigurations.FLINK.getConfNames()[0];
  public static final String CONF_FLINK_ENV = ServiceConfigurations.FLINK.getConfNames()[1];

  private final static ObjectMapper objectMapper = new ObjectMapper();

  @Override
  protected String getServiceName() {
    return Constants.Flink.SERVICE_NAME;
  }

  @Override
  protected Map<Component, List<ComponentProcess>> createComponents(Config config, Map<String, String> flattenConfigMap) {
    Map<Component, List<ComponentProcess>> components = new HashMap<>();

    Pair<Component, List<ComponentProcess>> connectionEndpoint = createBeamComponent(config, flattenConfigMap);
    components.put(connectionEndpoint.getFirst(), connectionEndpoint.getSecond());

    return Collections.unmodifiableMap(components);
  }

  @Override
  protected List<ServiceConfiguration> createServiceConfigurations(Config config) {
    ServiceConfiguration serverProperties = buildServerPropertiesServiceConfiguration(config);
    return ImmutableList.of(serverProperties);
  }

  @Override
  protected boolean validateComponents(Map<Component, List<ComponentProcess>> components) {
    //TODO add logic to validate beam components.
    return true;
  }

  @Override
  protected boolean validateServiceConfigurations(List<ServiceConfiguration> serviceConfigurations) {

    return true;
  }

  @Override
  protected boolean validateServiceConfiguationsAsFlattenedMap(Map<String, String> configMap) {
    return true;
  }

  private ServiceConfiguration buildServerPropertiesServiceConfiguration(Config config) {
    ServiceConfiguration serverProperties = new ServiceConfiguration();
    serverProperties.setName(FLINK);

    Map<String, String> confMap = new HashMap<>();

    if (config.contains(Constants.Flink.PROPERTY_CONNECTION_ENDPOINT)) {
      confMap.put(Constants.Flink.PROPERTY_CONNECTION_ENDPOINT, config.getString(Constants.Flink.PROPERTY_CONNECTION_ENDPOINT));
    }

    try {
      String json = objectMapper.writeValueAsString(confMap);
      serverProperties.setConfiguration(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Unable to parse beam service properties");
    }
    return serverProperties;
  }

  private Pair<Component, List<ComponentProcess>> createBeamComponent(Config config, Map<String, String> flattenConfigMap) {
    if (!config.contains(Constants.Flink.PROPERTY_CONNECTION_ENDPOINT)) {
      throw new IllegalArgumentException("Required parameter " + Constants.Flink.PROPERTY_CONNECTION_ENDPOINT + " not present.");
    }
    Component beamComponent = new Component();
    beamComponent.setName(Constants.Flink.PROPERTY_CONNECTION_ENDPOINT);

    List<ComponentProcess> componentProcesses = new ArrayList<>();

    return new Pair<>(beamComponent, componentProcesses);
  }
}
