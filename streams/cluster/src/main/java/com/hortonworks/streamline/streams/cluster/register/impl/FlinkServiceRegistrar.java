/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/
package com.hortonworks.streamline.streams.cluster.register.impl;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.google.common.collect.*;
import com.hortonworks.streamline.common.*;
import com.hortonworks.streamline.streams.cluster.Constants;
import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.common.ServiceConfigurations;
import org.apache.commons.math3.util.*;

import java.util.*;

public class FlinkServiceRegistrar extends AbstractServiceRegistrar {

  public static final String PARAM_CONNECTION_ENDPOINT = "master.endpoint";


  public static final String CONF_FLINK = ServiceConfigurations.FLINK.getConfNames()[0];
  public static final String CONF_FLINK_ENV = ServiceConfigurations.FLINK.getConfNames()[1];

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  protected String getServiceName() {
    return Constants.Flink.SERVICE_NAME;
  }

  @Override
  protected Map<Component, List<ComponentProcess>> createComponents(Config config,
      Map<String, String> flattenConfigMap) {
    Map<Component, List<ComponentProcess>> components = new HashMap<>();

    Pair<Component, List<ComponentProcess>> kafkaBroker = createFlinkComponent(config,
        flattenConfigMap);
    components.put(kafkaBroker.getFirst(), kafkaBroker.getSecond());

    return components;
  }

  @Override
  protected List<ServiceConfiguration> createServiceConfigurations(Config config) {
    ServiceConfiguration serverProperties = buildServerPropertiesServiceConfiguration(config);
    ServiceConfiguration flinkEnvProperties = buildFlinkEnvServiceConfiguration(config);
    return Lists.newArrayList(serverProperties, flinkEnvProperties);
  }

  @Override
  protected boolean validateComponents(Map<Component, List<ComponentProcess>> components) {
    return true;
  }

  @Override
  protected boolean validateServiceConfigurations(
      List<ServiceConfiguration> serviceConfigurations) {

    return true;
  }

  @Override
  protected boolean validateServiceConfiguationsAsFlattenedMap(Map<String, String> configMap) {
    return true;
  }

  private ServiceConfiguration buildServerPropertiesServiceConfiguration(Config config) {
    ServiceConfiguration serverProperties = new ServiceConfiguration();
    serverProperties.setName(CONF_FLINK);

    Map<String, String> confMap = new HashMap<>();

    if (config.contains(Constants.Flink.PROPERTY_CONNECTION_ENDPOINT)) {
      confMap.put(Constants.Flink.PROPERTY_CONNECTION_ENDPOINT, config.getString(Constants.Flink.PROPERTY_CONNECTION_ENDPOINT));
    }

    try {
      String json = objectMapper.writeValueAsString(confMap);
      serverProperties.setConfiguration(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return serverProperties;
  }

  private ServiceConfiguration buildFlinkEnvServiceConfiguration(Config config) {
    ServiceConfiguration serverProperties = new ServiceConfiguration();
    serverProperties.setName(CONF_FLINK_ENV);

    Map<String, String> confMap = new HashMap<>();

    try {
      String json = objectMapper.writeValueAsString(confMap);
      serverProperties.setConfiguration(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return serverProperties;
  }

  private Pair<Component, List<ComponentProcess>> createFlinkComponent(Config config,
      Map<String, String> flattenConfigMap) {
    if (!config.contains(PARAM_CONNECTION_ENDPOINT)) {
      throw new IllegalArgumentException(
          "Required parameter " + PARAM_CONNECTION_ENDPOINT + " not present.");
    }
    Component flinkComponent = new Component();
    flinkComponent.setName(Constants.Flink.PROPERTY_CONNECTION_ENDPOINT);

    List<ComponentProcess> componentProcesses = new ArrayList<>();

    return new Pair<>(flinkComponent, componentProcesses);
  }
}
