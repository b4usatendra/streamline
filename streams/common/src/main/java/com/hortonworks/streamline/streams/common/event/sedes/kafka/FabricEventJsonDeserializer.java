/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 **/
package com.hortonworks.streamline.streams.common.event.sedes.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FabricEventJsonDeserializer implements Deserializer<StreamlineEvent> {

    protected static final Logger LOG = LoggerFactory.getLogger(FabricEventJsonDeserializer.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private Map<String, ?> configs;

    public FabricEventJsonDeserializer() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configs = configs;
    }

    @Override
    public StreamlineEvent deserialize(String topic, byte[] bytes) {
        try {
            JsonNode jsonEvent = mapper.readTree(bytes);
            validateEvent(jsonEvent);

            String id = jsonEvent.get("id").toString();
            Map<String, Object> metadata = mapper.convertValue(jsonEvent.get("metadata"), Map.class);
            Map<String, Object> data = mapper.convertValue(jsonEvent.get("data"), Map.class);
            StreamlineEvent fabicEvent = StreamlineEventImpl.builder()
                .id(id)
                .header(metadata)
                .fieldsAndValues(data).build();
            return fabicEvent;
        } catch (IOException e) {
            LOG.error("Unable to convert event to JSON: " + e.getMessage());
        } catch (ProcessingException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }

    private void validateEvent(JsonNode jsonEvent) throws ProcessingException {
        if (jsonEvent.has("id") && jsonEvent.has("metadata") && jsonEvent.has("data")) {
            return;
        }
        throw new ProcessingException("Input Event does not comply with Fabric document structure");
    }

    @Override
    public void close() {
    }

}
