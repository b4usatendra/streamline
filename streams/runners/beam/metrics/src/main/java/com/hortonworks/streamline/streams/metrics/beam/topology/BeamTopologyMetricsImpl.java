package com.hortonworks.streamline.streams.metrics.beam.topology;

import com.google.common.base.Stopwatch;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.hortonworks.streamline.common.exception.ConfigException;
import com.hortonworks.streamline.common.util.ParallelStreamUtil;
import com.hortonworks.streamline.streams.beam.common.BeamRestAPIConstant;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyUtil;
import com.hortonworks.streamline.streams.cluster.service.EnvironmentService;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.component.Component;
import com.hortonworks.streamline.streams.layout.component.TopologyLayout;
import com.hortonworks.streamline.streams.metrics.TimeSeriesQuerier;
import com.hortonworks.streamline.streams.metrics.topology.TopologyMetrics;
import com.hortonworks.streamline.streams.metrics.topology.TopologyTimeSeriesMetrics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author satendra.sahu Beam implementation of the TopologyMetrics interface
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
    environmentService = (EnvironmentService) conf
        .get(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TopologyMetric getTopologyMetric(TopologyLayout topology, String asUser) {

    //TODO add misc stats(refer: StormTopologyMetricsImpl.jav)
    Map<String, Number> miscMetrics = new HashMap<>();
    miscMetrics.put("TOTAL_WORKERS", 1);


    return new TopologyMetric(FRAMEWORK, topology.getName(), "RUNNING", 1000l,
        1000l,
        1.0, 1.0, 0l, miscMetrics);
  }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, ComponentMetric> getMetricsForTopology(TopologyLayout topology,
        String asUser) {

        //TODO add extracted metrics
        Map<String, ComponentMetric> metricMap = new HashMap<>();

        if(topology.getTopologyDag()==null){
          return metricMap;
        }
        for (Component component : topology.getTopologyDag().getComponents()) {
            metricMap
                .put(component.getName(), new ComponentMetric(component.getName(), 0l, 0l, 0l, 0d));
        }
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
  public Map<Long, Double> getCompleteLatency(TopologyLayout topology, Component component,
      long from, long to, String asUser) {
    return timeSeriesMetrics.getCompleteLatency(topology, component, from, to, asUser);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, Map<Long, Double>> getkafkaTopicOffsets(TopologyLayout topology,
      Component component, long from, long to, String asUser) {
    return timeSeriesMetrics.getkafkaTopicOffsets(topology, component, from, to, asUser);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TimeSeriesComponentMetric getTopologyStats(TopologyLayout topology, long from, long to,
      String asUser) {
    return timeSeriesMetrics.getTopologyStats(topology, from, to, asUser);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TimeSeriesComponentMetric getComponentStats(TopologyLayout topology, Component component,
      long from, long to, String asUser) {
    return timeSeriesMetrics.getComponentStats(topology, component, from, to, asUser);
  }

  private long getErrorCountFromAllComponents(String topologyId, List<Map<String, ?>> spouts,
      List<Map<String, ?>> bolts, String asUser) {
    LOG.debug("[START] getErrorCountFromAllComponents - topology id: {}, asUser: {}", topologyId,
        asUser);
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
            List<?> componentErrors = (List<?>) componentStats
                .get(BeamRestAPIConstant.TOPOLOGY_JSON_COMPONENT_ERRORS);
            if (componentErrors != null && !componentErrors.isEmpty()) {
              return componentErrors.size();
            } else {
              return 0;
            }
          }).sum(), FORK_JOIN_POOL);

      LOG.debug("[END] getErrorCountFromAllComponents - topology id: {}, elapsed: {} ms",
          topologyId,
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return errorCount;
    } finally {
      stopwatch.stop();
    }
  }

  private void extractMetrics(Map<String, ComponentMetric> metricMap,
      List<Map<String, ?>> components, String topologyJsonID) {
    for (Map<String, ?> component : components) {
      String name = (String) component.get(topologyJsonID);
      String componentId = BeamTopologyUtil.extractStreamlineComponentId(name);
      ComponentMetric metric = extractMetric(name, component);
      metricMap.put(componentId, metric);
    }
  }

  private ComponentMetric extractMetric(String componentName, Map<String, ?> componentMap) {
    Long inputRecords = getLongValueOrDefault(componentMap,
        BeamRestAPIConstant.STATS_JSON_EXECUTED_TUPLES, 0L);
    Long outputRecords = getLongValueOrDefault(componentMap,
        BeamRestAPIConstant.STATS_JSON_EMITTED_TUPLES, 0L);
    Long failedRecords = getLongValueOrDefault(componentMap,
        BeamRestAPIConstant.STATS_JSON_FAILED_TUPLES, 0L);
    Double processedTime = getDoubleValueFromStringOrDefault(componentMap,
        BeamRestAPIConstant.STATS_JSON_PROCESS_LATENCY, 0.0d);

    return new ComponentMetric(BeamTopologyUtil.extractStreamlineComponentName(componentName),
        inputRecords,
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
    LOG.debug("[START] getComponentInfo - topology id: {}, component id: {}, asUser: {}",
        topologyId, componentId, asUser);
    Stopwatch stopwatch = Stopwatch.createStarted();

    try {
      Map<String, ?> responseMap;
      try {
        responseMap = componentRetrieveCache
            .get(new ImmutablePair<>(new ImmutablePair<>(topologyId, componentId), asUser));
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

      LOG.debug("[END] getComponentInfo - topology id: {}, component id: {}, elapsed: {} ms",
          topologyId,
          componentId, stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return responseMap;
    } finally {
      stopwatch.stop();
    }
  }
}