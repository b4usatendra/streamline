package com.hortonworks.streamline.streams.runtime.beam;

import com.hortonworks.streamline.streams.beam.common.*;
import com.hortonworks.streamline.streams.layout.beam.*;
import com.hortonworks.streamline.streams.layout.component.*;
import org.apache.beam.sdk.*;
import org.slf4j.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Created by Satendra Sahu on 11/28/18
 */
public class BeamPipelineExecutor {

    private Pipeline pipeline;
    private static final Logger LOG = LoggerFactory.getLogger(BeamPipelineExecutor.class);
    private String stormArtifactsLocation = "/tmp/storm-artifacts/";

    private void initializePipeline(){
        System.setProperty("java.security.auth.login.config", "/Users/satendra.sahu/code/github/streamline/conf/jaas.conf");

        TopologyLayout newLayout = deserializeTopologyDag();
        LOG.debug("Initial Topology config {}", newLayout.getConfig());
        Map<String, Object> conf= new HashMap<>();

        //-Dexec.args="--runner=DirectRunner"
        //System.setProperty("runner", "DirectRunner");
        TopologyDag topologyDag = newLayout.getTopologyDag();

        BeamTopologyFluxGenerator fluxGenerator = new BeamTopologyFluxGenerator(newLayout, conf, getExtraJarsLocation(newLayout));
        topologyDag.traverse(fluxGenerator);
        Pipeline pipeline = fluxGenerator.getPipeline();
        pipeline.run();
    }

    public Path getExtraJarsLocation(TopologyLayout topology) {
        return Paths.get(stormArtifactsLocation, generateBeamTopologyName(topology), "jars");
    }

    private String generateBeamTopologyName(TopologyLayout topology) {
        return BeamTopologyUtil.generateStormTopologyName(topology.getId(), topology.getName());
    }

    private TopologyLayout deserializeTopologyDag() {
        // Deserialization
        TopologyLayout topologyLayout = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream("/tmp/serialized_object");
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            topologyLayout = (TopologyLayout) in.readObject();

            in.close();
            file.close();

            System.out.println("Object has been deserialized ");
            System.out.println("a = " + topologyLayout.toString());
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }

        return topologyLayout;
    }


    public static void main(String[] args) {
        BeamPipelineExecutor executor = new BeamPipelineExecutor();
        executor.initializePipeline();
    }
}
