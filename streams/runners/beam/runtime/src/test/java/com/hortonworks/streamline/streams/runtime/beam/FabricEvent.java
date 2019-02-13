package com.hortonworks.streamline.streams.runtime.beam;

/**
 * Created by Satendra Sahu on 2/13/19
 */
public class FabricEvent {

    private String id;
    private Object metadata;
    private Object data;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Object getMetadata() {
        return metadata;
    }

    public void setMetadata(Object metadata) {
        this.metadata = metadata;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
