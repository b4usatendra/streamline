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

import com.google.common.base.*;
import com.google.common.cache.*;
import com.google.common.util.concurrent.*;
import com.hortonworks.streamline.common.exception.*;
import com.hortonworks.streamline.common.util.*;
import com.hortonworks.streamline.streams.beam.common.*;
import com.hortonworks.streamline.streams.cluster.service.*;
import com.hortonworks.streamline.streams.layout.*;
import com.hortonworks.streamline.streams.layout.component.*;
import com.hortonworks.streamline.streams.metrics.*;
import com.hortonworks.streamline.streams.metrics.topology.*;
import org.apache.commons.lang3.tuple.*;
import org.slf4j.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Beam implementation of the TopologyMetrics interface
 */
public class BeamTopologyMetricsImpl implements TopologyMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(BeamTopologyMetricsImpl.class);

    private static final String FRAMEWORK = "BEAM";
    private static final int MAX_SIZE_TOPOLOGY_CACHE = 10;
    private static final int MAX_SIZE_COMPONENT_CACHE = 50;
    private static final int CACHE_DURATION_SECS = 5;
    private static final int FORK_JOIN_POOL_PARALLELISM = 50;
    private Map<String, Object> conf;
    private EnvironmentService environmentService;
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
    public void init(Map<String, Object> conf)
            throws ConfigException {
        this.conf = conf;
        environmentService = (EnvironmentService) conf.get(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TopologyMetric getTopologyMetric(TopologyLayout topology, String asUser) {

        return new TopologyMetric(FRAMEWORK, topology.getName(), "TOPOLOGY_STATE_DEPLOYED", 1000l, 1000l,
                1.0, 1.0, 0l, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, ComponentMetric> getMetricsForTopology(TopologyLayout topology, String asUser) {

        Map<String, ?> responseMap = getTopologyInfo("test-topology", asUser);

        Map<String, ComponentMetric> metricMap = new HashMap<>();
        List<Map<String, ?>> spouts = (List<Map<String, ?>>) responseMap.get(BeamRestAPIConstant.TOPOLOGY_JSON_SPOUTS);
        extractMetrics(metricMap, spouts, BeamRestAPIConstant.TOPOLOGY_JSON_SPOUT_ID);

        List<Map<String, ?>> bolts = (List<Map<String, ?>>) responseMap.get(BeamRestAPIConstant.TOPOLOGY_JSON_BOLTS);
        extractMetrics(metricMap, bolts, BeamRestAPIConstant.TOPOLOGY_JSON_BOLT_ID);

        return metricMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTimeSeriesQuerier(TimeSeriesQuerier timeSeriesQuerier) {
        timeSeriesMetrics.setTimeSeriesQuerier(timeSeriesQuerier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesQuerier getTimeSeriesQuerier() {
        return timeSeriesMetrics.getTimeSeriesQuerier();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, Double> getCompleteLatency(TopologyLayout topology, Component component, long from, long to, String asUser) {
        return timeSeriesMetrics.getCompleteLatency(topology, component, from, to, asUser);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Map<Long, Double>> getkafkaTopicOffsets(TopologyLayout topology, Component component, long from, long to, String asUser) {
        return timeSeriesMetrics.getkafkaTopicOffsets(topology, component, from, to, asUser);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesComponentMetric getTopologyStats(TopologyLayout topology, long from, long to, String asUser) {
        return timeSeriesMetrics.getTopologyStats(topology, from, to, asUser);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesComponentMetric getComponentStats(TopologyLayout topology, Component component, long from, long to, String asUser) {
        return timeSeriesMetrics.getComponentStats(topology, component, from, to, asUser);
    }

    private long getErrorCountFromAllComponents(String topologyId, List<Map<String, ?>> spouts, List<Map<String, ?>> bolts, String asUser) {
        LOG.debug("[START] getErrorCountFromAllComponents - topology id: {}, asUser: {}", topologyId, asUser);
        Stopwatch stopwatch = Stopwatch.createStarted();

        try {
            List<String> componentIds = new ArrayList<>();

            if (spouts != null) {
                for (Map<String, ?> spout : spouts) {
                    componentIds.add((String) spout.get(BeamRestAPIConstant.TOPOLOGY_JSON_SPOUT_ID));
                }
            }

            if (bolts != null) {
                for (Map<String, ?> bolt : bolts) {
                    componentIds.add((String) bolt.get(BeamRestAPIConstant.TOPOLOGY_JSON_BOLT_ID));
                }
            }

            // query to components in parallel
            long errorCount = ParallelStreamUtil.execute(() ->
                    componentIds.parallelStream().mapToLong(componentId -> {
                        Map componentStats = getComponentInfo(topologyId, componentId, asUser);
                        List<?> componentErrors = (List<?>) componentStats.get(BeamRestAPIConstant.TOPOLOGY_JSON_COMPONENT_ERRORS);
                        if (componentErrors != null && !componentErrors.isEmpty()) {
                            return componentErrors.size();
                        } else {
                            return 0;
                        }
                    }).sum(), FORK_JOIN_POOL);

            LOG.debug("[END] getErrorCountFromAllComponents - topology id: {}, elapsed: {} ms", topologyId,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return errorCount;
        } finally {
            stopwatch.stop();
        }
    }

    private void extractMetrics(Map<String, ComponentMetric> metricMap, List<Map<String, ?>> components, String topologyJsonID) {
        for (Map<String, ?> component : components) {
            String name = (String) component.get(topologyJsonID);
            String componentId = BeamTopologyUtil.extractStreamlineComponentId(name);
            ComponentMetric metric = extractMetric(name, component);
            metricMap.put(componentId, metric);
        }
    }

    private ComponentMetric extractMetric(String componentName, Map<String, ?> componentMap) {
        Long inputRecords = getLongValueOrDefault(componentMap, BeamRestAPIConstant.STATS_JSON_EXECUTED_TUPLES, 0L);
        Long outputRecords = getLongValueOrDefault(componentMap, BeamRestAPIConstant.STATS_JSON_EMITTED_TUPLES, 0L);
        Long failedRecords = getLongValueOrDefault(componentMap, BeamRestAPIConstant.STATS_JSON_FAILED_TUPLES, 0L);
        Double processedTime = getDoubleValueFromStringOrDefault(componentMap, BeamRestAPIConstant.STATS_JSON_PROCESS_LATENCY, 0.0d);

        return new ComponentMetric(BeamTopologyUtil.extractStreamlineComponentName(componentName), inputRecords,
                outputRecords, failedRecords, processedTime);
    }

    private Long convertWindowString(String windowStr, Long uptime) {
        if (windowStr.equals(":all-time")) {
            return uptime;
        } else {
            return Long.valueOf(windowStr);
        }
    }

    private Long getLongValueOrDefault(Map<String, ?> map, String key, Long defaultValue) {
        if (map.containsKey(key)) {
            Number number = (Number) map.get(key);
            if (number != null) {
                return number.longValue();
            }
        }
        return defaultValue;
    }

    private Double getDoubleValueFromStringOrDefault(Map<String, ?> map, String key,
                                                     Double defaultValue) {
        if (map.containsKey(key)) {
            String valueStr = (String) map.get(key);
            if (valueStr != null) {
                try {
                    return Double.parseDouble(valueStr);
                } catch (NumberFormatException e) {
                    // noop
                }
            }
        }
        return defaultValue;
    }

    private Map<String, ?> getTopologyInfo(String topologyId, String asUser) {
        LOG.debug("[START] getTopologyInfo - topology id: {}, asUser: {}", topologyId, asUser);
        Stopwatch stopwatch = Stopwatch.createStarted();

        try {
            Map<String, ?> responseMap;
            try {
                responseMap = topologyRetrieveCache.get(new ImmutablePair<>(topologyId, asUser));
            } catch (ExecutionException e) {
                if (e.getCause() != null) {
                    throw new RuntimeException(e.getCause());
                } else {
                    throw new RuntimeException(e);
                }
            } catch (UncheckedExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new RuntimeException(e);
                }
            }

            LOG.debug("[END] getTopologyInfo - topology id: {}, elapsed: {} ms", topologyId,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return responseMap;
        } finally {
            stopwatch.stop();
        }
    }

    private Map<String, ?> getComponentInfo(String topologyId, String componentId, String asUser) {
        // FIXME: we still couldn't handle the case which contains auxiliary part on component name... how to handle?
        LOG.debug("[START] getComponentInfo - topology id: {}, component id: {}, asUser: {}", topologyId, componentId, asUser);
        Stopwatch stopwatch = Stopwatch.createStarted();

        try {
            Map<String, ?> responseMap;
            try {
                responseMap = componentRetrieveCache.get(new ImmutablePair<>(new ImmutablePair<>(topologyId, componentId), asUser));
            } catch (ExecutionException e) {
                if (e.getCause() != null) {
                    throw new RuntimeException(e.getCause());
                } else {
                    throw new RuntimeException(e);
                }
            } catch (UncheckedExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new RuntimeException(e);
                }
            }

            LOG.debug("[END] getComponentInfo - topology id: {}, component id: {}, elapsed: {} ms", topologyId,
                    componentId, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return responseMap;
        } finally {
            stopwatch.stop();
        }
    }
}