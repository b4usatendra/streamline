package com.hortonworks.streamline.streams.layout.event;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.*;

import java.io.*;
import java.util.*;

/**
 * Created by Satendra Sahu on 12/4/18
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Record implements Serializable {

    private long id;
    private Object data;

    @JsonIgnore
    private JsonNode jsonNode;
    private Map<String, String> properties;

    public Map<String, String> getProperties() {
        if (null == properties) {
            properties = new HashMap<>();
        }
        return properties;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public JsonNode getJsonNode() {
        return jsonNode;
    }

    public void setJsonNode(JsonNode jsonNode) {
        this.jsonNode = jsonNode;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }


}
