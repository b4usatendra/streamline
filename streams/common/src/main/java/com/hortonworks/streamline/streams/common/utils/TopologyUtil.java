/**
  * Copyright 2017 Hortonworks.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at

  *   http://www.apache.org/licenses/LICENSE-2.0

  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
 **/
package com.hortonworks.streamline.streams.common.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TopologyUtil {


    public static String generateStormTopologyName(Long topologyId, String topologyName) {
        return "streamline-" + topologyId + "-" + topologyName;
    }

    public static String generateStormComponentId(Long componentId, String componentName) {
        return String.format("%s-%s", componentId, componentName);
    }

    public static String generateUniqueStormTopologyNamePrefix(Long topologyId) {
        return "streamline-" + topologyId + "-";
    }

    public static String extractStreamlineComponentName(String stormComponentId) {
        String[] splitted = stormComponentId.split("-");
        if (splitted.length <= 1) {
            throw new IllegalArgumentException("Invalid Storm component ID for Streamline: " + stormComponentId);
        }

        List<String> splittedList = Arrays.asList(splitted);
        return String.join("-", splittedList.subList(1, splittedList.size()));
    }

    public static String extractStreamlineComponentId(String stormComponentId) {
        // removes all starting from first '-'
        return stormComponentId.substring(0, stormComponentId.indexOf('-'));
    }
}
