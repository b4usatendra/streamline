package com.hortonworks.streamline.streams.cluster.catalog;


import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.common.Schema.Field;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.annotation.StorableEntity;
import com.hortonworks.registries.storage.catalog.AbstractStorable;
import java.util.HashMap;
import java.util.Map;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Created by Satendra Sahu on 2/6/19. Stores the mapping of running job/pipeline/topology with following entities: streamlingEngine topologyId ApplicationMasterURL/clusterEndpoint.
 */

@StorableEntity
public class JobClusterMap extends AbstractStorable {

    public static final String NAMESPACE = "job_cluster_map";
    public static final String ID = "id";
    public static final String TOPOLOGYID = "topologyId";
    public static final String TOPOLOGYNAME = "topologyName";
    public static final String STREAMINGENGINE = "streamingEngine";
    public static final String JOBID = "job_id";
    public static final String CONNECTIONENDPOINT = "connectionEndpoint";


    private Long id;
    private Long topologyId;
    private String topologyName;
    private String streamingEngine;
    private String jobId;
    private String connectionEndpoint;


    /**
     * Storage namespace this can be translated to a jdbc table or zookeeper node or hbase table. TODO: Namespace can be a first class entity, probably needs its own class.
     *
     * @return the namespace
     */
    @Override
    @JsonIgnore
    public String getNameSpace() {
        return NAMESPACE;
    }

    /**
     * Defines set of columns that uniquely identifies this storage entity. This can be translated to a primary key of a table. of fields.
     *
     * @return the primary key
     */
    @Override
    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        Map<Field, Object> fieldToObjectMap = new HashMap<>();
        fieldToObjectMap.put(new Schema.Field("id", Schema.Type.LONG), this.id);
        return new PrimaryKey(fieldToObjectMap);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getTopologyId() {
        return topologyId;
    }

    public void setTopologyId(Long topologyId) {
        this.topologyId = topologyId;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public String getStreamingEngine() {
        return streamingEngine;
    }

    public void setStreamingEngine(String streamingEngine) {
        this.streamingEngine = streamingEngine;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getConnectionEndpoint() {
        return connectionEndpoint;
    }

    public void setConnectionEndpoint(String connectionEndpoint) {
        this.connectionEndpoint = connectionEndpoint;
    }
}
