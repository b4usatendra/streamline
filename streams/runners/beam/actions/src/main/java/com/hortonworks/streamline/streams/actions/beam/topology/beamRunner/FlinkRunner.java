package com.hortonworks.streamline.streams.actions.beam.topology.beamRunner;

import com.hortonworks.streamline.common.exception.WrappedWebApplicationException;
import com.hortonworks.streamline.streams.actions.TopologyActionContext;
import com.hortonworks.streamline.streams.actions.beam.topology.beamRunner.config.BeamAbstractRunner;
import com.hortonworks.streamline.streams.beam.common.BeamRunner;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.beam.common.FlinkRestAPIClient;
import com.hortonworks.streamline.streams.catalog.TopologyTestRunHistory;
import com.hortonworks.streamline.streams.cluster.catalog.JobClusterMap;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.beam.TopologyMapper;
import com.hortonworks.streamline.streams.layout.component.TopologyLayout;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.ClientBuilder;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException;
import org.glassfish.jersey.client.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Satendra Sahu on 4/23/19
 */
public class FlinkRunner extends BeamAbstractRunner {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkRunner.class);
    private FlinkRestAPIClient flinkClient;
    private String flinkExec;

    public FlinkRunner() {
    }

    @Override
    public void init(Map<String, Object> conf) {
        super.init(conf);
        String flinkCliPath = (String) conf.get(BeamTopologyLayoutConstants.FLINK_HOME_DIR);
        flinkExec = flinkCliPath + "bin/flink";
    }

    @Override
    public void deploy(TopologyLayout topology, String mavenArtifacts, TopologyActionContext ctx,
        String asUser) throws Exception {
        Collection<JobClusterMap> jobClusterMaps = environmentService
            .getJobByTopologyName(topology.getName(), BeamRunner.FLINK.name());

        //Preconditions before deploying
        if (jobClusterMaps.size() != 0) {
            JobClusterMap jobClusterMap = jobClusterMaps.iterator().next();
            throw new AlreadyExistsException(String
                .format("Flink Job with name: {} and jobId: {} already exists.", topology.getName(),
                    jobClusterMap.getJobId()));
        }

        String runnerMasterCmd = getRunnerMaster();
        Map<String, Object> config = new HashMap<String, Object>();

        //serialize topology
        File file = new File(getTopologyTempPath(topology.getName(), "serialization"));
        file.mkdirs();

        addJaaSConfig(topology.getName());

        String filePath =
            file.getAbsolutePath() + BeamTopologyLayoutConstants.TOPOLOGY_SERIALIZED_OBJECT_PATH;

        Set<Entry<String, Object>> set = conf.entrySet();

        for (Map.Entry<String, Object> entry : set) {
            if (entry.getValue() instanceof String) {
                config.put(entry.getKey(), entry.getValue());
            }
        }
        serializeTopologyDag(new TopologyMapper(config, topology), filePath);

        //add JAAS config



        ctx.setCurrentAction("Adding artifacts to jar");
        //downloadArtifactsAndCopyJars(mavenArtifacts, getExtraJarsLocation(topology));
        Path jarToDeploy = addArtifactsToJar(getExtraJarsLocation(topology));
        //addJaaSConfig(conf, topology.getName(), jarToDeploy);

        List<String> commands = new ArrayList<String>();
        JobClusterMap jobClusterMap = new JobClusterMap();
        jobClusterMap.setTopologyName(topology.getName());
        jobClusterMap.setTopologyId(topology.getId());
        jobClusterMap.setRunner(BeamRunner.FLINK.name());

        ctx.setCurrentAction("Creating Flink Pipeline from topologyDAG");

        //Always add key value pair in command in separate line.
        //TODO make command args configurable.
        if (runnerMasterCmd.contains("[auto]")) {
            commands.add(flinkExec);
            commands.add("run");
            //commands.add(TopologyLayoutConstants.SHUTDOWN_HOOK);
            commands.add("-m");
            commands.add(BeamTopologyLayoutConstants.YARN_CLUSTER);

            commands.add(BeamTopologyLayoutConstants.YARN_CONTAINER);
            commands.add(
                topology.getConfig().get(BeamTopologyLayoutConstants.JSON_KEY_YARN_CONTAINER, "2"));

            commands.add(BeamTopologyLayoutConstants.YARN_SLOTS);
            commands.add(
                topology.getConfig().get(BeamTopologyLayoutConstants.JSON_KEY_YARN_SLOTS, "4"));

            commands.add(BeamTopologyLayoutConstants.YARN_JOB_MANAGER_MEMORY);
            commands.add(topology.getConfig()
                .get(BeamTopologyLayoutConstants.JSON_KEY_YARN_JOB_MANAGER_MEMORY, "2000"));

            commands.add(BeamTopologyLayoutConstants.YARN_TASK_MANAGER_MEMORY);
            commands.add(topology.getConfig()
                .get(BeamTopologyLayoutConstants.JSON_KEY_YARN_TASK_MANAGER_MEMORY, "2000"));

            commands.add(BeamTopologyLayoutConstants.YARN_NAME);
            commands.add(topology.getName());
            commands.add(jarToDeploy.toString());
            commands.add(filePath);
            commands.add(BeamTopologyLayoutConstants.RUNNER + "=" + BeamRunner.FLINK.name());
            commands.add(BeamTopologyLayoutConstants.FLINK_MASTER + "=" + runnerMasterCmd);
            commands.add(BeamTopologyLayoutConstants.FILES_TO_STAGE + "=" + jarToDeploy);
        } else {
            commands.add("java");
            commands.add("-jar");
            commands.add(jarToDeploy.toString());
            commands.add(filePath);
            commands.add(BeamTopologyLayoutConstants.RUNNER + "=" + BeamRunner.FLINK.name());
            commands.add(BeamTopologyLayoutConstants.FILES_TO_STAGE + "=" + jarToDeploy);
            String connectionEndpoint = runnerMasterCmd.replace("http://", "");
            commands.add(BeamTopologyLayoutConstants.FLINK_MASTER + "=" + connectionEndpoint);
            jobClusterMap.setConnectionEndpoint(connectionEndpoint);
        }

        LOG.info("Deploying Application {}", topology.getName());
        LOG.info(String.join(" ", commands));
        Process process = executeShellProcess(commands);
        ShellProcessResult shellProcessResult = waitProcessFor(process);

        if (shellProcessResult.exitValue != 0) {
            LOG.error("Topology deploy command failed - exit code: {} / output: {}",
                shellProcessResult.exitValue, shellProcessResult.stdout);
            String[] lines = shellProcessResult.stdout.split("\\n");
            String errors = Arrays.stream(lines)
                .filter(line -> line.startsWith("Exception") || line.startsWith("Caused by"))
                .collect(Collectors.joining(", "));
            throw new Exception(
                "Topology could not be deployed successfully: flink deploy command failed with "
                    + errors);
        } else {
            updateJobClusterMapping(shellProcessResult.stdout, jobClusterMap);
        }

        environmentService.addJobClusterMapping(jobClusterMap);
    }

    private String getRunnerMaster() {
        if (!beamRunnerConfig.containsKey(BeamTopologyLayoutConstants.FLINK_MASTER_KEY)) {
            return "[auto]";
        }
        return beamRunnerConfig.get(BeamTopologyLayoutConstants.FLINK_MASTER_KEY);
    }


    @Override
    public boolean killTest(TopologyTestRunHistory testRunHistory) {
        // just turn on the flag only if it exists
        throw new NotImplementedException("Kill Test is not Implemented for Flink Runner");
    }

    @Override
    public void kill(TopologyLayout topology, String asUser) throws Exception {

        Iterator<JobClusterMap> iterator = environmentService
            .getJobByTopologyName(topology.getName(), BeamRunner.FLINK.name())
            .iterator();

        LOG.info("List running topologies with name: {} on streamingEngine: {}", topology.getName(),
            BeamRunner.FLINK.name());

        if (!beamRunnerConfig.containsKey(BeamTopologyLayoutConstants.FLINK_MASTER_KEY)) {
            if (iterator.hasNext()) {
                JobClusterMap jobClusterMap = iterator.next();
                LOG.info("Killing topology {} running on {}", topology.getName(),
                    jobClusterMap.getConnectionEndpoint());
                List<String> commands = new ArrayList<String>();
                commands.add("yarn");
                commands.add("application");
                commands.add("-kill");
                commands.add(jobClusterMap.getJobId());

                Process process = executeShellProcess(commands);
                ShellProcessResult shellProcessResult = waitProcessFor(process);
                if (shellProcessResult.exitValue != 0) {
                    LOG.error("Unable to kill Flink ApplicationMaster - exit code: {} / output: {}",
                        shellProcessResult.exitValue, shellProcessResult.stdout);
                    String[] lines = shellProcessResult.stdout.split("\\n");
                    String errors = Arrays.stream(lines)
                        .filter(
                            line -> line.startsWith("Exception") || line.startsWith("Caused by"))
                        .collect(Collectors.joining(", "));
                    throw new Exception(
                        "Unable to kill Flink ApplicationMaster failed with "
                            + errors);
                } else {
                    environmentService.removeJobClusterMapping(jobClusterMap.getId());
                }
            }
        } else {
            if (iterator.hasNext()) {
                JobClusterMap jobClusterMap = iterator.next();
                LOG.info("Killing topology {} running on {}", topology.getName(),
                    jobClusterMap.getConnectionEndpoint());
                flinkClient = new FlinkRestAPIClient(
                    String.format("http://%s", jobClusterMap.getConnectionEndpoint()),
                    (Subject) conf.get(TopologyLayoutConstants.SUBJECT_OBJECT),
                    ClientBuilder.newClient(new ClientConfig()));
                String jobId = null;
                try {
                    jobId = flinkClient.getJobIdByName(topology.getName());
                } catch (WrappedWebApplicationException | NotFoundException e) {
                    LOG.error(String.format("Topology {} not found: error {}", topology.getName()),
                        e.getMessage());
                }
                if (jobId != null) {
                    flinkClient.stopJob(jobId);
                    environmentService.removeJobClusterMapping(jobClusterMap.getId());
                }
            }
        }
    }


    @Override
    public void suspend(TopologyLayout topology, String asUser) throws Exception {

    }

    @Override
    public void resume(TopologyLayout topology, String asUser) throws Exception {
        throw new NotImplementedException("RESUME functionality for FLINK is not implemented");
    }

    @Override
    public Status status(TopologyLayout topology, String asUser) throws Exception {
        return null;
    }

    /**
     * the topology id which is running in runtime streaming engine
     */
    @Override
    public String getRuntimeTopologyId(TopologyLayout topology, String asUser) {
        //TODO get the runtime topology id(calls cluster apis)
        Iterator<JobClusterMap> iterator = environmentService
            .getJobByTopologyName(topology.getName(), BeamRunner.FLINK.name())
            .iterator();
        if (iterator.hasNext()) {
            return iterator.next().getJobId();
        }
        return null;
    }
}
