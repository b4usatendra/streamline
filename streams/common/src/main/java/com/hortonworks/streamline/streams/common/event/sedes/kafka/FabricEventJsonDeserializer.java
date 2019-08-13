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
package com.hortonworks.streamline.streams.common.event.sedes.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.streamline.streams.common.Constants;
import com.hortonworks.streamline.streams.common.FabricEventImpl;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FabricEventJsonDeserializer implements Deserializer<FabricEventImpl> {

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
    public FabricEventImpl deserialize(String topic, byte[] bytes) {
        try {
            JsonNode jsonEvent = mapper.readTree(bytes);
            validateEvent(jsonEvent);

            String id = jsonEvent.get(Constants.FABRIC_ID).toString();
            Map<String, Object> metadata = mapper
                .convertValue(jsonEvent.get(Constants.FABRIC_METADATA), Map.class);
            Map<String, Object> data = mapper
                .convertValue(jsonEvent.get(Constants.FABRIC_DATA), Map.class);

            return FabricEventImpl.builder()
                .id(id)
                .dataSourceId((String) metadata.get(Constants.FABRIC_METADATA_SENDER))
                .sourceStream(topic)
                .header(metadata)
                .fieldsAndValues(data)
                .build();

        } catch (IOException e) {
            LOG.error("Unable to convert event to JSON: " + e.getMessage());
        } catch (ProcessingException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }

    private void validateEvent(JsonNode jsonEvent) throws ProcessingException {

        boolean isValid = true;
        StringBuffer validationExceptions = new StringBuffer();
        if (!jsonEvent.has(Constants.FABRIC_ID) || !jsonEvent.has(Constants.FABRIC_METADATA)
            || !jsonEvent.has(Constants.FABRIC_DATA)) {
            validationExceptions
                .append("Either 'id', 'metadata' or 'data' section is missing in the Event\n");
            isValid = false;
        }

        if (jsonEvent.has(Constants.FABRIC_METADATA)) {
            JsonNode metadataNode = jsonEvent.get(Constants.FABRIC_METADATA);

            if (!metadataNode.has(Constants.FABRIC_METADATA_TIMESTAMP) ||
                !metadataNode.has(Constants.FABRIC_METADATA_SCHEMA) ||
                !metadataNode.has(Constants.FABRIC_METADATA_SCHEMA_VERSION) ||
                !metadataNode.has(Constants.FABRIC_METADATA_TYPE) ||
                !metadataNode.has(Constants.FABRIC_METADATA_ROUTING_KEY) ||
                !metadataNode.has(Constants.FABRIC_METADATA_LOOKUP_KEY) ||
                !metadataNode.has(Constants.FABRIC_METADATA_TENANT) ||
                !metadataNode.has(Constants.FABRIC_METADATA_STREAM) ||
                !metadataNode.has(Constants.FABRIC_METADATA_SENDER)) {
                isValid = false;
            }
            validationExceptions
                .append("Missing fields from METADATA\n");


        }

        if (!isValid) {
            throw new ProcessingException(validationExceptions.toString());
        }
    }

    @Override
    public void close() {
    }

}
