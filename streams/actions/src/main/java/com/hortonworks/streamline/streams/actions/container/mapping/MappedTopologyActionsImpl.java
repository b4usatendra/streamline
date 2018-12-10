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
package com.hortonworks.streamline.streams.actions.container.mapping;
public enum MappedTopologyActionsImpl {
    STORM("com.hortonworks.streamline.streams.actions.storm.topology.StormTopologyActionsImpl"),
    BEAM("com.hortonworks.streamline.streams.actions.beam.topology.BeamTopologyActionsImpl");

    private final String className;

    MappedTopologyActionsImpl(String className) {
        this.className = className;
    }

    public String getClassName() {
        return className;
    }
}
