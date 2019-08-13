/**
 * Copyright 2017 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 **/
package com.hortonworks.streamline.streams.common.event.sedes.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.Constants;
import com.hortonworks.streamline.streams.common.FabricEventImpl;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FabricEventJsonSerializer implements Serializer<FabricEventImpl> {

    protected static final Logger LOG = LoggerFactory.getLogger(FabricEventJsonSerializer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public FabricEventJsonSerializer() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, FabricEventImpl streamlineEvent) {
        ObjectNode fabricEvent = JsonNodeFactory.instance.objectNode();
        fabricEvent.put(Constants.FABRIC_ID, streamlineEvent.getId());

        Map<String, Object> newHeader = new HashMap<>();
        newHeader.putAll(streamlineEvent.getHeader());
        newHeader.put(Constants.FABRIC_METADATA_STREAM, topic);

        fabricEvent.putPOJO(Constants.FABRIC_METADATA, mapper.convertValue(newHeader, JsonNode.class));
        fabricEvent.putPOJO(Constants.FABRIC_DATA, mapper.convertValue(streamlineEvent.getFieldsAndValues(), JsonNode.class));

        try {
            return mapper.writeValueAsString(fabricEvent).getBytes();
        } catch (JsonProcessingException e) {
            LOG.error("unable to convert event to Fabric Doc: {}", e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {
    }
}
