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
package com.hortonworks.streamline.streams.service;

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.*;
import com.hortonworks.streamline.streams.actions.topology.service.*;
import com.hortonworks.streamline.streams.actions.topology.state.*;
import com.hortonworks.streamline.streams.catalog.*;
import com.hortonworks.streamline.streams.catalog.service.*;
import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.service.*;
import com.hortonworks.streamline.streams.exception.*;
import com.hortonworks.streamline.streams.metrics.topology.*;
import com.hortonworks.streamline.streams.metrics.topology.service.*;
import com.hortonworks.streamline.streams.storm.common.*;
import org.apache.commons.lang3.tuple.*;
import org.slf4j.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public final class CatalogResourceUtil
{
   private static final Logger LOG = LoggerFactory.getLogger(CatalogResourceUtil.class);
   private static final Integer DEFAULT_N_OF_TOP_N_LATENCY = 3;

   private CatalogResourceUtil()
   {
   }

   @JsonInclude(JsonInclude.Include.NON_NULL)
   static class TopologyDashboardResponse
   {
	  private final Topology topology;
	  private final TopologyRunningStatus running;
	  private final String namespaceName;
	  private TopologyRuntimeResponse runtime;

	  public TopologyDashboardResponse(Topology topology, TopologyRunningStatus running, String namespaceName)
	  {
		 this.topology = topology;
		 this.running = running;
		 this.namespaceName = namespaceName;
	  }

	  public void setRuntime(TopologyRuntimeResponse runtime)
	  {
		 this.runtime = runtime;
	  }

	  public Topology getTopology()
	  {
		 return topology;
	  }

	  public TopologyRunningStatus getRunning()
	  {
		 return running;
	  }

	  public String getNamespaceName()
	  {
		 return namespaceName;
	  }

	  public TopologyRuntimeResponse getRuntime()
	  {
		 return runtime;
	  }
   }

   @JsonInclude(JsonInclude.Include.NON_NULL)
   static class TopologyRuntimeResponse
   {
	  private final String runtimeTopologyId;
	  private final TopologyMetrics.TopologyMetric metric;
	  private final List<Pair<String, Double>> latencyTopN;

	  public TopologyRuntimeResponse(String runtimeTopologyId, TopologyMetrics.TopologyMetric metric, List<Pair<String, Double>> latencyTopN)
	  {
		 this.runtimeTopologyId = runtimeTopologyId;
		 this.metric = metric;
		 this.latencyTopN = latencyTopN;
	  }

	  public String getRuntimeTopologyId()
	  {
		 return runtimeTopologyId;
	  }

	  public TopologyMetrics.TopologyMetric getMetric()
	  {
		 return metric;
	  }

	  public List<Pair<String, Double>> getLatencyTopN()
	  {
		 return latencyTopN;
	  }
   }

   enum TopologyRunningStatus
   {
	  RUNNING, NOT_RUNNING, UNKNOWN
   }

   static TopologyDashboardResponse enrichTopology(Topology topology,
		   String asUser,
		   Integer latencyTopN,
		   EnvironmentService environmentService,
		   TopologyActionsService actionsService,
		   TopologyMetricsService metricsService,
		   StreamCatalogService catalogService)
   {
	  LOG.debug("[START] enrichTopology - topology id: {}", topology.getId());
	  Stopwatch stopwatch = Stopwatch.createStarted();

	  try
	  {
		 if (latencyTopN == null)
		 {
			latencyTopN = DEFAULT_N_OF_TOP_N_LATENCY;
		 }

		 TopologyDashboardResponse detailedResponse;

		 String namespaceName = null;
		 Namespace namespace = environmentService.getNamespace(topology.getNamespaceId());
		 if (namespace != null)
		 {
			namespaceName = namespace.getName();
		 }

		 try
		 {
			String runtimeTopologyId = actionsService.getRuntimeTopologyId(topology, asUser);
			TopologyMetrics.TopologyMetric topologyMetric = metricsService.getTopologyMetric(topology, asUser);

			//TODO make topology state configurable
			detailedResponse = new TopologyDashboardResponse(topology, TopologyRunningStatus.NOT_RUNNING, namespaceName);

			if (!namespace.getStreamingEngine().equalsIgnoreCase("beam")) {
			   List<Pair<String, Double>> latenciesTopN = metricsService.getTopNAndOtherComponentsLatency(topology, asUser, latencyTopN);
			   detailedResponse.setRuntime(new TopologyRuntimeResponse(runtimeTopologyId, topologyMetric, latenciesTopN));
			}

		 }
		 catch (TopologyNotAliveException e)
		 {
			LOG.debug("Topology {} is not alive", topology.getId());
			detailedResponse = new TopologyDashboardResponse(topology, TopologyRunningStatus.NOT_RUNNING, namespaceName);
			catalogService.getTopologyState(topology.getId())
					.ifPresent(state -> {
					   if (TopologyStateFactory.getInstance().getTopologyState(state.getName()) == TopologyStates.TOPOLOGY_STATE_DEPLOYED)
					   {
						  try
						  {
							 LOG.info("Force killing streamline topology since its not alive in the cluster");
							 actionsService.killTopology(topology, asUser);
						  }
						  catch (Exception ex)
						  {
							 LOG.error("Error trying to kill topology", ex);
						  }
					   }
					});
		 }
		 catch (StormNotReachableException | IOException e)
		 {
			LOG.error("Storm is not reachable or fail to operate", e);
			detailedResponse = new TopologyDashboardResponse(topology, TopologyRunningStatus.UNKNOWN, namespaceName);
		 }
		 catch (Exception e)
		 {
			LOG.error("Unhandled exception occurs while operate with Storm", e);
			detailedResponse = new TopologyDashboardResponse(topology, TopologyRunningStatus.UNKNOWN, namespaceName);
		 }

		 LOG.debug("[END] enrichTopology - topology id: {}, elapsed: {} ms", topology.getId(),
				 stopwatch.elapsed(TimeUnit.MILLISECONDS));

		 return detailedResponse;
	  }
	  finally
	  {
		 stopwatch.stop();
	  }
   }

   static class NamespaceWithMapping
   {
	  private Namespace namespace;
	  private Collection<NamespaceServiceClusterMap> mappings = new ArrayList<>();

	  public NamespaceWithMapping(Namespace namespace)
	  {
		 this.namespace = namespace;
	  }

	  public Namespace getNamespace()
	  {
		 return namespace;
	  }

	  public Collection<NamespaceServiceClusterMap> getMappings()
	  {
		 return mappings;
	  }

	  public void setServiceClusterMappings(Collection<NamespaceServiceClusterMap> mappings)
	  {
		 this.mappings = mappings;
	  }

	  public void addServiceClusterMapping(NamespaceServiceClusterMap mapping)
	  {
		 mappings.add(mapping);
	  }
   }

   static NamespaceWithMapping enrichNamespace(Namespace namespace,
		   EnvironmentService environmentService)
   {
	  NamespaceWithMapping nm = new NamespaceWithMapping(namespace);
	  nm.setServiceClusterMappings(environmentService.listServiceClusterMapping(namespace.getId()));
	  return nm;
   }
}
