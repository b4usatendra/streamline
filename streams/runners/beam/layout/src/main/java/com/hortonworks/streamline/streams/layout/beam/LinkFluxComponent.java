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
package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.common.exception.ComponentConfigException;
import com.hortonworks.streamline.streams.beam.common.*;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import org.apache.beam.sdk.values.PCollection;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Implementation for flux streams
 */
public class LinkFluxComponent extends AbstractBeamComponent {


    protected void updateLinkComponentWithGrouping(Map<String, Object> groupingInfo) {
        if (groupingInfo != null && !groupingInfo.isEmpty()) {
            component.put(TopologyLayoutConstants.YAML_KEY_GROUPING, groupingInfo);
        }
    }

    protected Map<String, Object> getGroupingYamlForType(String groupingType) {
        Map<String, Object> grouping = new LinkedHashMap<>();
        grouping.put(TopologyLayoutConstants.YAML_KEY_TYPE, groupingType);
        if (conf.get(TopologyLayoutConstants.JSON_KEY_STREAM_ID) != null) {
            grouping.put(TopologyLayoutConstants.YAML_KEY_STREAM_ID, conf.get(TopologyLayoutConstants.JSON_KEY_STREAM_ID));
        }
        return grouping;
    }

    @Override
    public PCollection getOutputCollection() {
        return null;
    }

    @Override
    public void generateComponent(PCollection pCollection) {
        String linkName = "link" + UUID_FOR_COMPONENTS;
        component.put(TopologyLayoutConstants.YAML_KEY_NAME, linkName);
        component.put(TopologyLayoutConstants.YAML_KEY_FROM, conf.get
                (TopologyLayoutConstants.JSON_KEY_FROM));
        component.put(TopologyLayoutConstants.YAML_KEY_TO, conf.get
                (TopologyLayoutConstants.JSON_KEY_TO));
    }

    @Override
    public void unionInputCollection(PCollection collection) {

    }

    @Override
    public void validateConfig() throws ComponentConfigException {
        validateStringFields();
    }

    private void validateStringFields() throws ComponentConfigException {
        String[] requiredStringFields = {
                TopologyLayoutConstants.JSON_KEY_FROM,
                TopologyLayoutConstants.JSON_KEY_TO
        };
        validateStringFields(requiredStringFields, true);
        String[] optionalStringFields = {
                TopologyLayoutConstants.JSON_KEY_STREAM_ID
        };
        validateStringFields(optionalStringFields, false);
    }

}
