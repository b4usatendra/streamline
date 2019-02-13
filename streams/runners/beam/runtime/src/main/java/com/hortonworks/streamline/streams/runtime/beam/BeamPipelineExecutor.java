package com.hortonworks.streamline.streams.runtime.beam;

import com.hortonworks.streamline.streams.beam.common.BeamRunner;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyUtil;
import com.hortonworks.streamline.streams.layout.beam.BeamTopologyComponentGenerator;
import com.hortonworks.streamline.streams.layout.beam.TopologyMapper;
import com.hortonworks.streamline.streams.layout.component.TopologyDag;
import com.hortonworks.streamline.streams.layout.component.TopologyLayout;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Satendra Sahu on 11/28/18
 */
public class BeamPipelineExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(BeamPipelineExecutor.class);
    private String beamArtifactsLocation;

    private void initializePipeline(String filePath, PipelineOptions options) {

        TopologyMapper topologyMapper = deserializeTopologyDag(filePath);
        TopologyLayout newLayout = topologyMapper.getTopologyLayout();
        LOG.info("Initial Topology config {}", newLayout.getConfig());
        Map<String, Object> conf = topologyMapper.getConf();
        beamArtifactsLocation = (String) conf.get(BeamTopologyLayoutConstants.BEAM_ARTIFACTS_LOCATION_KEY);
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
        } catch (IOException ex) {
            LOG.error("Unable to de-serialize topology: " + ex.getMessage());
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            LOG.error("ClassNotFoundException: " + ex.getMessage());
        }

        return topologyMapper;
    }


    public static void main(String[] args) {
        String filePath = args[0];
        BeamPipelineExecutor executor = new BeamPipelineExecutor();
        args = Arrays.copyOfRange(args, 1, args.length);

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        if (options.getRunner().getSimpleName().equalsIgnoreCase(BeamRunner.FlinkRunner.name())) {
            options.as(FlinkPipelineOptions.class);
        }

        executor.initializePipeline(filePath, options);
    }
}
