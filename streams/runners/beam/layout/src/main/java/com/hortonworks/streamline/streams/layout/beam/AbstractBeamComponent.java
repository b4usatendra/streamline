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
package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.common.exception.ComponentConfigException;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.component.ComponentUtils;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Abstract implementation of BeamComponent interface. Child classes just need to implement
 * generateComponent method using conf and update referenced components and component variables
 */
public abstract class AbstractBeamComponent extends ComponentUtils implements BeamComponent {

  protected final UUID UUID_FOR_COMPONENTS = UUID.randomUUID();
  // conf is the map representing the configuration parameters for this
  // storm component picked by the user. For eg kafka component will have
  // zkUrl, topic, etc.
  protected Map<String, Object> conf;
  protected boolean isGenerated;
  protected transient PCollection<StreamlineEvent> outputCollection;
  protected transient Pipeline pipeline;
  protected Map<String, Object> component = new LinkedHashMap<>();

  @Override
  public void withCatalogRootUrl(String catalogRootUrl) {
    throw new NotImplementedException();
//        this.catalogRestClient = new CatalogRestClient(catalogRootUrl);
  }

  @Override
  public void withConfig(Map<String, Object> conf, Pipeline pipeline) {
    this.conf = conf;
    this.pipeline = pipeline;
  }

  @Override
  public abstract PCollection<StreamlineEvent> getOutputCollection();

  @Override
  public void unionInputCollection(PCollection<StreamlineEvent> collection) {
    outputCollection = PCollectionList.of(outputCollection).and(collection)
        .apply(Flatten.<StreamlineEvent>pCollections());
  }


  // validate boolean fields based on if they are required or not. Meant to
  // be called from base classes that need to validate
  protected void validateBooleanFields(String[] fieldNames, boolean areRequiredFields)
      throws ComponentConfigException {
    validateBooleanFields(fieldNames, areRequiredFields, conf);
  }

  // validate string fields based on if they are required or not. Meant to
  // be called from base classes that need to validate
  protected void validateStringFields(String[] fieldNames, boolean areRequiredFields)
      throws ComponentConfigException {
    validateStringFields(fieldNames, areRequiredFields, conf);
  }

  // validate integer fields based on if they are required or not and their
  // valid range. Meant to // be called from base classes that need to validate
  protected void validateIntegerFields(String[] fieldNames, boolean areRequiredFields,
      Integer[] mins, Integer[] maxes) throws ComponentConfigException {
    this.validateIntegerFields(fieldNames, areRequiredFields, mins, maxes, conf);
  }

  protected void validateFloatOrDoubleFields(String[] requiredFields, boolean areRequiredFields)
      throws ComponentConfigException {
    this.validateFloatOrDoubleFields(requiredFields, areRequiredFields, conf);
  }

}