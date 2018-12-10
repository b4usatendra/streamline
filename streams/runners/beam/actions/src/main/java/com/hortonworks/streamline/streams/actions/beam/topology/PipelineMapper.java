package com.hortonworks.streamline.streams.actions.beam.topology;

import org.apache.beam.sdk.*;

import java.io.*;
import java.nio.channels.*;

/**
 * Created by Satendra Sahu on 11/22/18
 */
public class PipelineMapper implements Serializable
{
   private Pipeline pipeline;

   public PipelineMapper(Pipeline pipeline){
      this.pipeline = pipeline;
   }

   public Pipeline getPipeline(){
      return pipeline;
   }
}
