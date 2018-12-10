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
package com.hortonworks.streamline.streams.metrics.beam.topology;

import com.google.common.cache.*;
import com.hortonworks.streamline.common.exception.*;
import com.hortonworks.streamline.streams.layout.*;
import com.hortonworks.streamline.streams.layout.component.*;
import com.hortonworks.streamline.streams.metrics.*;
import com.hortonworks.streamline.streams.metrics.topology.TopologyMetrics;
import com.hortonworks.streamline.streams.metrics.topology.TopologyTimeSeriesMetrics;
import org.apache.commons.lang3.tuple.*;
import org.slf4j.*;

import javax.security.auth.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Storm implementation of the TopologyMetrics interface
 */
public class BeamTopologyMetricsImpl implements TopologyMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(BeamTopologyMetricsImpl.class);

    private static final String FRAMEWORK = "STORM";
    private static final int MAX_SIZE_TOPOLOGY_CACHE = 10;
    private static final int MAX_SIZE_COMPONENT_CACHE = 50;
    private static final int CACHE_DURATION_SECS = 5;
    private static final int FORK_JOIN_POOL_PARALLELISM = 50;

    // shared across the metrics instances
    private static final ForkJoinPool FORK_JOIN_POOL = new ForkJoinPool(FORK_JOIN_POOL_PARALLELISM);

    private TopologyTimeSeriesMetrics timeSeriesMetrics;

    private LoadingCache<Pair<String, String>, Map<String, ?>> topologyRetrieveCache;
    private LoadingCache<Pair<Pair<String, String>, String>, Map<String, ?>> componentRetrieveCache;

    public BeamTopologyMetricsImpl() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(Map<String, Object> conf) throws ConfigException {
        String stormApiRootUrl = null;
        Subject subject = null;
        if (conf != null) {
            stormApiRootUrl = (String) conf.get(TopologyLayoutConstants.STORM_API_ROOT_URL_KEY);
            subject = (Subject) conf.get(TopologyLayoutConstants.SUBJECT_OBJECT);
        }

    }

    /**
     * Retrieves topology metric for Streamline topology/
     *
     * @param topology topology catalog instance. Implementations should find actual runtime topology with provided topology.
     * @param asUser   username if request needs impersonation to specific user
     * @return TopologyMetrics
     */
    @Override
    public TopologyMetric getTopologyMetric(TopologyLayout topology, String asUser) {
        return null;
    }


    /**
     * Retrieves metrics data for Streamline topology.
     *
     * @param topology topology catalog instance. Implementations should find actual runtime topology with provided topology.
     * @param asUser   username if request needs impersonation to specific user
     * @return pair of (component id, ComponentMetric instance).
     *         Implementations should ensure that component name is same to UI name of component
     *         so that it can be matched to Streamline topology.
     */
    @Override
    public Map<String, ComponentMetric> getMetricsForTopology(TopologyLayout topology, String asUser) {
        return null;
    }

    /**
     * Set instance of TimeSeriesQuerier. This method should be called before calling any requests for metrics.
     *
     * @param timeSeriesQuerier
     */
    @Override
    public void setTimeSeriesQuerier(TimeSeriesQuerier timeSeriesQuerier) {

    }

    /**
     * Retrieve "topology stats" on topology.
     * Implementator should aggregate all components' metrics values to make topology stats.
     * The return value is a TimeSeriesComponentMetric which value of componentName is dummy or topology name.
     *
     * @param topology topology catalog instance
     * @param from     beginning of the time period: timestamp (in milliseconds)
     * @param to       end of the time period: timestamp (in milliseconds)
     * @param asUser   username if request needs impersonation to specific user
     * @return Map of metric name and Map of data points which are paired to (timestamp, value).
     */
    @Override
    public TimeSeriesComponentMetric getTopologyStats(TopologyLayout topology, long from, long to, String asUser) {
        return null;
    }

    /**
     * Retrieve "complete latency" on source.
     *
     * @param topology  topology catalog instance
     * @param component component layout instance
     * @param from      beginning of the time period: timestamp (in milliseconds)
     * @param to        end of the time period: timestamp (in milliseconds)
     * @param asUser    username if request needs impersonation to specific user
     * @return Map of data points which are paired to (timestamp, value)
     */
    @Override
    public Map<Long, Double> getCompleteLatency(TopologyLayout topology, Component component, long from, long to, String asUser) {
        return null;
    }

    /**
     * Retrieve "kafka topic offsets" on source.
     * <p/>
     * This method retrieves three metrics,<br/>
     * 1) "logsize": sum of partition's available offsets for all partitions<br/>
     * 2) "offset": sum of source's current offsets for all partitions<br/>
     * 3) "lag": sum of lags (available offset - current offset) for all partitions<br/>
     * <p/>
     * That source should be "KAFKA" type and have topic name from configuration.
     *
     * @param topology  topology layout instance
     * @param component component layout instance
     * @param from      beginning of the time period: timestamp (in milliseconds)
     * @param to        end of the time period: timestamp (in milliseconds)
     * @param asUser    username if request needs impersonation to specific user
     * @return Map of metric name and Map of data points which are paired to (timestamp, value).
     */
    @Override
    public Map<String, Map<Long, Double>> getkafkaTopicOffsets(TopologyLayout topology, Component component, long from, long to, String asUser) {
        return null;
    }

    /**
     * Retrieve "component stats" on component.
     *
     * @param topology  topology catalog instance
     * @param component component layout instance
     * @param from      beginning of the time period: timestamp (in milliseconds)
     * @param to        end of the time period: timestamp (in milliseconds)
     * @param asUser    username if request needs impersonation to specific user
     * @return Map of metric name and Map of data points which are paired to (timestamp, value).
     */
    @Override
    public TimeSeriesComponentMetric getComponentStats(TopologyLayout topology, Component component, long from, long to, String asUser) {
        return null;
    }

    /**
     * Get instance of TimeSeriesQuerier.
     */
    @Override
    public TimeSeriesQuerier getTimeSeriesQuerier() {
        return null;
    }
}