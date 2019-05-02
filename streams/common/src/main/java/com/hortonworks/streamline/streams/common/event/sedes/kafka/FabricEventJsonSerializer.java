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
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FabricEventJsonSerializer implements Serializer<StreamlineEvent> {

    protected static final Logger LOG = LoggerFactory.getLogger(FabricEventJsonSerializer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public FabricEventJsonSerializer() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, StreamlineEvent streamlineEvent) {
        ObjectNode fabricEvent = JsonNodeFactory.instance.objectNode();
        fabricEvent.put("id", streamlineEvent.getId());
        fabricEvent.put("metadata", mapper.convertValue(streamlineEvent.getHeader(), JsonNode.class));
        fabricEvent.put("data", mapper.convertValue(streamlineEvent.getFieldsAndValues(), JsonNode.class));
        try {
            return mapper.writeValueAsString(fabricEvent).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private SchemaMetadata getSchemaKey(String topic, boolean isKey) {
        String name = isKey ? topic + ":k" : topic;
        return new SchemaMetadata.Builder(name).type(AvroSchemaProvider.TYPE).schemaGroup("kafka").build();
    }

    @Override
    public void close() {
    }
}
