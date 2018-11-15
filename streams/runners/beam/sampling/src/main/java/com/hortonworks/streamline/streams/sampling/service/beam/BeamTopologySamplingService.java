package com.hortonworks.streamline.streams.sampling.service.beam;

import com.hortonworks.streamline.streams.catalog.*;
import com.hortonworks.streamline.streams.sampling.service.*;
import org.slf4j.*;

import java.util.*;

public class BeamTopologySamplingService implements TopologySampling
{
   private static final Logger LOG = LoggerFactory.getLogger(BeamTopologySamplingService.class);

   public BeamTopologySamplingService()
   {
   }

   @Override
   public void init(Map<String, Object> conf)
   {
	  System.out.println("");
   }

   @Override
   public boolean enableSampling(Topology topology, int pct, String asUser)
   {

	  return true;
   }

   @Override
   public boolean enableSampling(Topology topology, TopologyComponent component, int pct, String asUser)
   {
	  return true;
   }

   @Override
   public boolean disableSampling(Topology topology, String asUser)
   {
	  return true;
   }

   @Override
   public boolean disableSampling(Topology topology, TopologyComponent component, String asUser)
   {
	  return true;
   }

   @Override
   public SamplingStatus getSamplingStatus(Topology topology, String asUser)
   {

	  return buildSamplingStatus(null);
   }

   @Override
   public SamplingStatus getSamplingStatus(Topology topology, TopologyComponent component, String asUser)
   {
	  return buildSamplingStatus(null);
   }

   private SamplingStatus buildSamplingStatus(Map result)
   {
	  return result == null ? null : new SamplingStatus()
	  {
		 @Override
		 public Boolean getEnabled()
		 {
			Object debug = result.get("debug");
			return debug != null && debug instanceof Boolean ? (Boolean) debug : false;
		 }

		 @Override
		 public Integer getPct()
		 {
			Object samplingPct = result.get("samplingPct");
			return samplingPct != null && samplingPct instanceof Number ? ((Number) samplingPct).intValue() : 0;
		 }
	  };
   }
}
