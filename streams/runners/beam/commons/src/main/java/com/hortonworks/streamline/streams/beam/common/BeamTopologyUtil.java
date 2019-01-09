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
package com.hortonworks.streamline.streams.beam.common;

import com.hortonworks.streamline.streams.common.utils.TopologyUtil;
import org.apache.beam.repackaged.beam_sdks_java_core.com.google.common.base.Strings;

public class BeamTopologyUtil extends TopologyUtil {

  private BeamTopologyUtil() {
    super();
  }

  //TODO add beam client
  public static String findBeamTopologyId(Long topologyId, String asUser) {
    String topologyNamePrefix = generateUniqueStormTopologyNamePrefix(topologyId);
    String beamTopologyId = topologyNamePrefix;

    return beamTopologyId;
  }

  public static String findStormCompleteTopologyName(Long topologyId, String asUser) {
    String topologyNamePrefix = generateUniqueStormTopologyNamePrefix(topologyId);

    return topologyNamePrefix;
  }

  public static String findOrGenerateTopologyName(Long topologyId, String topologyName, String asUser) {
    String actualTopologyName = topologyName + topologyId;
    if (Strings.isNullOrEmpty(actualTopologyName)) {
      actualTopologyName = generateStormTopologyName(topologyId, topologyName);
    }
    return actualTopologyName;
  }

}
