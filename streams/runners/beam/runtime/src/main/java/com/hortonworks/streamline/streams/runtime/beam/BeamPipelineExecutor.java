package com.hortonworks.streamline.streams.runtime.beam;

import com.hortonworks.streamline.streams.beam.common.*;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.beam.*;
import com.hortonworks.streamline.streams.layout.component.*;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Created by Satendra Sahu on 11/28/18
 */
public class BeamPipelineExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(BeamPipelineExecutor.class);
    private String beamArtifactsLocation;

    private void initializePipeline(String filePath, PipelineOptions options) {
        //System.setProperty("java.security.auth.login.config", "/Users/satendra.sahu/code/github/streamline/conf/jaas.conf");

        TopologyMapper topologyMapper = deserializeTopologyDag(filePath);
        TopologyLayout newLayout = topologyMapper.getTopologyLayout();
        LOG.debug("Initial Topology config {}", newLayout.getConfig());
        Map<String, Object> conf = topologyMapper.getConf();
        beamArtifactsLocation= (String) conf.get(BeamTopologyLayoutConstants.BEAM_ARTIFACTS_LOCATION_KEY);
        //-Dexec.args="--runner=DirectRunner"
        //System.setProperty("runner", "DirectRunner");
        TopologyDag topologyDag = newLayout.getTopologyDag();

        BeamTopologyComponentGenerator fluxGenerator = new BeamTopologyComponentGenerator(newLayout, conf, getExtraJarsLocation(newLayout));
        topologyDag.traverse(fluxGenerator);
        Pipeline pipeline = fluxGenerator.getPipeline();

        //pipeline.getOptions().setRunner(FlinkRunner.class);
        //options.setTempLocation("/streamline/libs/beam-artifacts/streamline-4-kafka-example/jars");
        options.setJobName(topologyMapper.getTopologyLayout().getName());
        pipeline.run(options);

    }

    public Path getExtraJarsLocation(TopologyLayout topology) {
        return Paths.get(beamArtifactsLocation, generateBeamTopologyName(topology), BeamTopologyLayoutConstants.BEAM_JAR_FILE_TYPE_KEY);
    }

    private String generateBeamTopologyName(TopologyLayout topology) {
        return BeamTopologyUtil.generateBeamTopologyName(topology.getId(), topology.getName());
    }

    private TopologyMapper deserializeTopologyDag(String filePath) {
        // Deserialization
        TopologyMapper topologyMapper = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            topologyMapper = (TopologyMapper) in.readObject();

            in.close();
            file.close();

            System.out.println("Object has been deserialized ");
            System.out.println("a = " + topologyMapper.toString());
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }

        return topologyMapper;
    }


    public static void main(String[] args) {
        String filePath = args[0];
        BeamPipelineExecutor executor = new BeamPipelineExecutor();
        args=Arrays.copyOfRange(args,1,args.length);

        PipelineOptions options= PipelineOptionsFactory.fromArgs(args).withValidation().create();
        if(options.getRunner().getSimpleName().equalsIgnoreCase(BeamRunner.FlinkRunner.name()))
            options.as(FlinkPipelineOptions.class);

        executor.initializePipeline(filePath,options);
    }
}
