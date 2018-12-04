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
package com.hortonworks.streamline.streams.layout.component;

import com.fasterxml.jackson.core.type.*;
import com.fasterxml.jackson.databind.*;
import com.hortonworks.streamline.common.*;
import org.apache.commons.lang3.*;

import java.io.*;
import java.util.*;

public class TopologyLayout implements Serializable {
    private final Long id;
    private final String name;
    private final Config config;
    private final TopologyDag topologyDag;

    public TopologyLayout(Long id, String name, String configStr, TopologyDag topologyDag) throws IOException {
        this.id = id;
        this.name = name;
        if (!StringUtils.isEmpty(configStr)) {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> properties = mapper.readValue(configStr, new TypeReference<Map<String, Object>>() {
            });
            config = new Config();
            config.putAll(properties);
        } else {
            config = new Config();
        }
        this.topologyDag = topologyDag;
    }

    public TopologyLayout(Long id, String name, Config config, TopologyDag topologyDag) {
        this.id = id;
        this.name = name;
        this.config = config;
        this.topologyDag = topologyDag;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Config getConfig() {
        return config;
    }

    public TopologyDag getTopologyDag() {
        return topologyDag;
    }
}
