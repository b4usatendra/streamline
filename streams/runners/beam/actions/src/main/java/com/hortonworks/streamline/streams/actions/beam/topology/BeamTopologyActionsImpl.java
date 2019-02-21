/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/
package com.hortonworks.streamline.streams.actions.beam.topology;

import com.google.common.base.Joiner;
import com.hortonworks.streamline.common.exception.WrappedWebApplicationException;
import com.hortonworks.streamline.streams.actions.TopologyActionContext;
import com.hortonworks.streamline.streams.actions.TopologyActions;
import com.hortonworks.streamline.streams.actions.topology.service.ArtifactoryJarPathResolver;
import com.hortonworks.streamline.streams.actions.topology.service.HttpFileDownloader;
import com.hortonworks.streamline.streams.beam.common.BeamRunner;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyUtil;
import com.hortonworks.streamline.streams.beam.common.FlinkRestAPIClient;
import com.hortonworks.streamline.streams.catalog.TopologyTestRunHistory;
import com.hortonworks.streamline.streams.cluster.catalog.JobClusterMap;
import com.hortonworks.streamline.streams.cluster.catalog.Service;
import com.hortonworks.streamline.streams.cluster.service.EnvironmentService;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.beam.TopologyMapper;
import com.hortonworks.streamline.streams.layout.component.TopologyDag;
import com.hortonworks.streamline.streams.layout.component.TopologyLayout;
import com.hortonworks.streamline.streams.layout.component.impl.testing.TestRunProcessor;
import com.hortonworks.streamline.streams.layout.component.impl.testing.TestRunRulesProcessor;
import com.hortonworks.streamline.streams.layout.component.impl.testing.TestRunSink;
import com.hortonworks.streamline.streams.layout.component.impl.testing.TestRunSource;
import groovyx.net.http.HttpResponseException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.ClientBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException;
import org.glassfish.jersey.client.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beam implementation of the TopologyActions interface
 */
public class BeamTopologyActionsImpl implements TopologyActions {

    private static final Logger LOG = LoggerFactory.getLogger(BeamTopologyActionsImpl.class);

    private static final String DEFAULT_BEAM_ARTIFACTS_LOCATION = "/tmp/beam-artifacts";
    private static final String FLINK_MASTER_KEY = "master.endpoint";
    private final String tempTopologyPath = "/tmp/topology/%s/%s/";
    private static final String TOPOLOGY_SERIALIZED_OBJECT_PATH = "/serializedObject";
    private static final String FLINK_HOME_DIR = "flinkHomeDir";
    private FlinkRestAPIClient flinkClient;
    private String beamJarLocation;
    private final ConcurrentHashMap<Long, Boolean> forceKillRequests = new ConcurrentHashMap<>();
    private HttpFileDownloader httpFileDownloader;
    private String beamArtifactsLocation = DEFAULT_BEAM_ARTIFACTS_LOCATION;
    private String javaJarCommand;
    private BeamServiceConfigurationReader serviceConfigurationReader;
    private Map<String, Object> conf;
    private EnvironmentService environmentService;
    private Service beamRunnerService;
    Map<String, String> beamRunnerConfig;

    public BeamTopologyActionsImpl() {
    }

    @Override
    public void init(Map<String, Object> conf) {
        this.conf = conf;
        if (conf != null) {
            if (conf.containsKey(BeamTopologyLayoutConstants.BEAM_ARTIFACTS_LOCATION_KEY)) {
                beamArtifactsLocation = (String) conf
                    .get(BeamTopologyLayoutConstants.BEAM_ARTIFACTS_LOCATION_KEY);
            }

            this.beamJarLocation = (String) conf
                .get(BeamTopologyLayoutConstants.BEAM_PIPELINE_JAR_LOCATION_KEY);
            Map<String, String> env = System.getenv();
            String javaHomeStr = env.get("JAVA_HOME");
            if (StringUtils.isNotEmpty(javaHomeStr)) {
                if (!javaHomeStr.endsWith(File.separator)) {
                    javaHomeStr += File.separator;
                }
                javaJarCommand = javaHomeStr + "bin" + File.separator + "jar";
            } else {
                javaJarCommand = "jar";
            }
            setupSecuredCluster(conf);

            environmentService = (EnvironmentService) conf
                .get(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT);
            Number namespaceId = (Number) conf.get(TopologyLayoutConstants.NAMESPACE_ID);

            serviceConfigurationReader = new BeamServiceConfigurationReader(environmentService,
                namespaceId.longValue());

            beamRunnerService = (Service) conf.get(TopologyLayoutConstants.BEAM_RUNNER_SERVICE);
            beamRunnerConfig = serviceConfigurationReader.getRunnerConfig(beamRunnerService);

            try {
                httpFileDownloader = new HttpFileDownloader("tmp");
            } catch (Exception e) {
                e.printStackTrace();
            }

            File f = new File(beamArtifactsLocation);
            if (!f.exists() && !f.mkdirs()) {
                throw new RuntimeException("Could not create directory " + f.getAbsolutePath());
            }
        }
    }

    private void setupSecuredCluster(Map<String, Object> conf) {

    }


    @Override
    public void deploy(TopologyLayout topology, String mavenArtifacts, TopologyActionContext ctx,
        String asUser)
        throws Exception {
        Collection<JobClusterMap> jobClusterMaps = environmentService
            .getJobByTopologyName(topology.getName(), beamRunnerService.getName());
        //Preconditions before deploying

        if (jobClusterMaps.size() != 0) {
            JobClusterMap jobClusterMap = jobClusterMaps.iterator().next();
            throw new AlreadyExistsException(String
                .format("Flink Job with name: {} and jobId: {} already exists.", topology.getName(),
                    jobClusterMap.getJobId()));
        }

        BeamRunner beamRunnerInstance = BeamRunner.valueOf(beamRunnerService.getName());
        String runnerMasterCmd = getRunnerMaster(beamRunnerInstance);
        Map<String, Object> config = new HashMap<String, Object>();

        //serialize topology
        File file = new File(getTopologyTempPath(topology.getName(), "serialization"));
        file.mkdirs();
        String filePath = file.getAbsolutePath() + TOPOLOGY_SERIALIZED_OBJECT_PATH;

        Set<Map.Entry<String, Object>> set = conf.entrySet();

        for (Map.Entry<String, Object> entry : set) {
            if (entry.getValue() instanceof String) {
                config.put(entry.getKey(), entry.getValue());
            }
        }
        serializeTopologyDag(new TopologyMapper(config, topology), filePath);
        ctx.setCurrentAction("Adding artifacts to jar");
        //downloadArtifactsAndCopyJars(mavenArtifacts, getExtraJarsLocation(topology));
        Path jarToDeploy = addArtifactsToJar(getExtraJarsLocation(topology));
        //addJaaSConfig(conf, topology.getName(), jarToDeploy);

        List<String> commands = new ArrayList<String>();
        JobClusterMap jobClusterMap = new JobClusterMap();
        jobClusterMap.setTopologyName(topology.getName());
        jobClusterMap.setTopologyId(topology.getId());

        if (beamRunnerService.getName().equals(BeamRunner.FLINK.name())) {
            jobClusterMap.setRunner(beamRunnerService.getName());
            ctx.setCurrentAction("Creating Flink Pipeline from topology");
            String flinkCliPath = (String) conf.get(FLINK_HOME_DIR);
            String flinkExec = flinkCliPath + "bin/flink";

            //Always add key value pair in command in separate line.
            //TODO make command args configurable.
            if (runnerMasterCmd.contains("[auto]")) {
                commands.add(flinkExec);
                commands.add("run");
                //commands.add("-sae");
                commands.add("-m");
                commands.add("yarn-cluster");
                commands.add("--yarncontainer");
                commands.add((String) conf.get(BeamTopologyLayoutConstants.YARN_CONTAINER));
                commands.add("--yarnslots");
                commands.add((String) conf.get(BeamTopologyLayoutConstants.YARN_SLOTS));
                commands.add("--yarnjobManagerMemory");
                commands
                    .add((String) conf.get(BeamTopologyLayoutConstants.YARN_JOB_MANAGER_MEMORY));
                commands.add("--yarntaskManagerMemory");
                commands
                    .add((String) conf.get(BeamTopologyLayoutConstants.YARN_TASK_MANAGER_MEMORY));
                commands.add("--yarnname");
                commands.add(topology.getName());
                commands.add(jarToDeploy.toString());
                commands.add(filePath);
                commands.add("--runner=" + BeamRunner.getRunnerName(beamRunnerInstance.name()));
                commands.add("--flinkMaster=" + runnerMasterCmd);
                commands.add("--filesToStage=" + jarToDeploy);
            } else {
                commands.add("java");
                commands.add("-jar");
                commands.add(jarToDeploy.toString());
                commands.add(filePath);
                commands.add("--runner=" + BeamRunner.getRunnerName(beamRunnerInstance.name()));
                commands.add("--filesToStage=" + jarToDeploy);
                String connectionEndpoint = runnerMasterCmd.replace("http://", "");
                commands.add("--flinkMaster=" + connectionEndpoint);
                jobClusterMap.setConnectionEndpoint(connectionEndpoint);
            }
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

    private void updateJobClusterMapping(String stdout, JobClusterMap jobClusterMap) {
        String[] output = stdout.split("\\n\\t");

        if (!beamRunnerConfig.containsKey(FLINK_MASTER_KEY)) {
            for (String line : output) {

                if (line.contains("Connecting to ResourceManager at")) {
                    String[] splittedLines = line.split(" ");
                    String str = splittedLines[splittedLines.length - 1];
                    String[] strs = str.split("/");
                    String applicationHost = strs[0].trim();
                    String applicationPort = strs[1].split(":")[1];
                    jobClusterMap.setConnectionEndpoint(applicationHost + ":" + applicationPort);
                }

                if (line.contains("Submitted application ")) {
                    String[] splittedLines = line.split(" ");
                    String applicationId = splittedLines[splittedLines.length - 1];
                    applicationId.trim();
                    jobClusterMap.setJobId(applicationId);
                }
            }
        } else {
            for (String line : output) {
                if (line.contains("Submitting job")) {
                    String[] splittedLines = line.split(" ");
                    String applicationId = splittedLines[6];
                    jobClusterMap.setJobId(applicationId);
                }
            }
        }
    }


    private void addJaaSConfig(Map<String, Object> conf, String topologyName, Path jarToDeploy)
        throws Exception {
        BeamTopologyActionUtils.validateSSLConfig(conf);
        String jaasConfig = BeamTopologyActionUtils.getSaslJaasConfig(conf);
        File topologyMetaDir = new File(getTopologyTempPath(topologyName, "jaas"));
        topologyMetaDir.mkdirs();
        String jaasConfTempPath = topologyMetaDir.getAbsolutePath() + "/jaas.conf";
        BeamTopologyActionUtils.createJaasFile(jaasConfTempPath, jaasConfig);

        List<String> commands = new ArrayList<String>();

        commands.add("jar");
        commands.add("-uf");
        commands.add(jarToDeploy.toString());
        commands.add(jaasConfTempPath);

        LOG.info(String.join(" ", commands));
        Process process = executeShellProcess(commands);
        ShellProcessResult shellProcessResult = waitProcessFor(process);
        if (shellProcessResult.exitValue != 0) {
            LOG.error("unable to add JAAS File to jar - exit code: {} / output: {}",
                shellProcessResult.exitValue, shellProcessResult.stdout);
            String[] lines = shellProcessResult.stdout.split("\\n");
            String errors = Arrays.stream(lines)
                .filter(line -> line.startsWith("Exception") || line.startsWith("Caused by"))
                .collect(Collectors.joining(", "));
            throw new Exception(
                "Unable to add JAAS File to jar "
                    + errors);
        }

    }

    private BeamRunner getRunnerArg() {
        switch (beamRunnerService.getName()) {
            case "FLINK":
                return BeamRunner.FLINK;
            default:
                throw new UnsupportedOperationException(
                    String.format("Unsupported runner: %s", beamRunnerService.getName()));
        }
    }

    private String getRunnerMaster(BeamRunner runner) {
        switch (runner.name()) {
            case "FLINK":
                if (!beamRunnerConfig.containsKey(FLINK_MASTER_KEY)) {
                    return "[auto]";
                }
                return beamRunnerConfig.get(FLINK_MASTER_KEY);

            default:
                return null;
        }
    }

    private static class ShellProcessResult {

        private final int exitValue;
        private final String stdout;

        ShellProcessResult(int exitValue, String stdout) {
            this.exitValue = exitValue;
            this.stdout = stdout;
        }
    }

    private Process executeShellProcess(List<String> commands) throws Exception {
        LOG.info("Executing command: {}", Joiner.on(" ").join(commands));
        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        processBuilder.redirectErrorStream(true);
        return processBuilder.start();
    }

    private ShellProcessResult waitProcessFor(Process process)
        throws IOException, InterruptedException {
        StringWriter sw = new StringWriter();

        int exitValue = 0;
        process.waitFor(1, TimeUnit.SECONDS);
        LOG.info("waiting for the process to exit");
        if (!process.isAlive() && process.exitValue() == -1) {
            exitValue = process.exitValue();
            IOUtils.copy(process.getInputStream(), sw, Charset.defaultCharset());
        } else {
            final BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                sw.append(line).append("\n\t");
                System.out.println(line);
                if (line.contains("Submitting job") || line.contains("Submitted application ")) {
                    break;
                }
            }
        }

        String stdout = sw.toString();
        LOG.info("Command output: {}", stdout);
        LOG.info("Command exit status: {}", exitValue);
        return new ShellProcessResult(exitValue, stdout);
    }


    private void downloadArtifactsAndCopyJars(String mavenDeps, Path destinationPath) {
        String[] mavenDependencies = mavenDeps.split(",");
        String mavenRepo = (String) conf.get("mavenRepoUrl");
        String[] mavenRepositories = mavenRepo.split(",");
        Set<String> isDownloadedSet = new HashSet<>();
        for (String url : mavenRepositories) {
            url = url.split("\\^")[1];
            ArtifactoryJarPathResolver artifactoryJarPathResolver = new ArtifactoryJarPathResolver(
                url);
            for (String dependency : mavenDependencies) {
                if (!isDownloadedSet.contains(dependency)) {
                    String[] dependencyArray = dependency.split(":");
                    try {
                        String downloadUrl = artifactoryJarPathResolver
                            .resolve(dependencyArray[0], dependencyArray[1], dependencyArray[2]);
                        Path path = httpFileDownloader.download(downloadUrl);
                        FileUtils.copyFileToDirectory(new File(path.toAbsolutePath().toString()),
                            new File(destinationPath.toAbsolutePath().toString()));
                        isDownloadedSet.add(dependency);
                    } catch (HttpResponseException e) {
                        LOG.warn("{} not found in repository {}", dependency, url);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        //ArtifactoryJarPathResolver.resolve()
    }

    private List<String> getExtraJarsArg(TopologyLayout topology) {
        List<String> args = new ArrayList<>();
        List<String> jars = new ArrayList<>();
        Path extraJarsPath = getExtraJarsLocation(topology);
        if (extraJarsPath.toFile().isDirectory()) {
            File[] jarFiles = extraJarsPath.toFile().listFiles();
            if (jarFiles != null && jarFiles.length > 0) {
                for (File jarFile : jarFiles) {
                    jars.add(jarFile.getAbsolutePath());
                }
            }
        } else {
            LOG.debug("Extra jars directory {} does not exist, not adding any extra jars",
                extraJarsPath);
        }
        if (!jars.isEmpty()) {
            args.add("--jars");
            args.add(Joiner.on(",").join(jars));
        }
        return args;
    }

    private Path addArtifactsToJar(Path artifactsLocation) throws Exception {
        Path jarFile = Paths.get(beamJarLocation);
   /* if (artifactsLocation.toFile().isDirectory()) {
      File[] artifacts = artifactsLocation.toFile().listFiles();
      if (artifacts != null && artifacts.length > 0) {
        Path newJar = Files.copy(jarFile, artifactsLocation.resolve(jarFile.getFileName()));

        List<String> artifactFileNames = Arrays.stream(artifacts).filter(File::isFile)
            .map(File::getName).collect(toList());
        List<String> commands = new ArrayList<>();

        commands.add(javaJarCommand);
        commands.add("uf");
        commands.add(newJar.toString());

        artifactFileNames.stream().forEachOrdered(name -> {
          commands.add("-C");
          commands.add(artifactsLocation.toString());
          commands.add(name);
        });

        Process process = executeShellProcess(commands);
        ShellProcessResult shellProcessResult = waitProcessFor(process, null);
        if (shellProcessResult.exitValue != 0) {
          LOG.error("Adding artifacts to jar command failed - exit code: {} / output: {}",
              shellProcessResult.exitValue, shellProcessResult.stdout);
          throw new RuntimeException("Topology could not be deployed " +
              "successfully: fail to add artifacts to jar");
        }
        LOG.debug("Added files {} to jar {}", artifactFileNames, jarFile);
        return newJar;
      }
    } else {
      LOG.debug("Artifacts directory {} does not exist, not adding any artifacts to jar",
          artifactsLocation);
    }*/
        return jarFile;
    }

    @Override
    public void runTest(TopologyLayout topology, TopologyTestRunHistory testRunHistory,
        String mavenArtifacts,
        Map<String, TestRunSource> testRunSourcesForEachSource,
        Map<String, TestRunProcessor> testRunProcessorsForEachProcessor,
        Map<String, TestRunRulesProcessor> testRunRulesProcessorsForEachProcessor,
        Map<String, TestRunSink> testRunSinksForEachSink, Optional<Long> durationSecs)
        throws Exception {
        TopologyDag originalTopologyDag = topology.getTopologyDag();

        TestTopologyDagCreatingVisitor visitor = new TestTopologyDagCreatingVisitor(
            originalTopologyDag,
            testRunSourcesForEachSource, testRunProcessorsForEachProcessor,
            testRunRulesProcessorsForEachProcessor,
            testRunSinksForEachSink);
        originalTopologyDag.traverse(visitor);
    }

    @Override
    public boolean killTest(TopologyTestRunHistory testRunHistory) {
        // just turn on the flag only if it exists
        LOG.info("Turning on force kill flag on test run history {}", testRunHistory.getId());
        Boolean newValue = forceKillRequests
            .computeIfPresent(testRunHistory.getId(), (id, flag) -> true);
        return newValue != null;
    }

    @Override
    public void kill(TopologyLayout topology, String asUser)
        throws Exception {

        Iterator<JobClusterMap> iterator = environmentService
            .getJobByTopologyName(topology.getName(), beamRunnerService.getName())
            .iterator();
        LOG.info("List running topologies with name: {} on streamingEngine: {}", topology.getName(),
            beamRunnerService.getName());
        if (!beamRunnerConfig.containsKey(FLINK_MASTER_KEY)) {
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
    public void validate(TopologyLayout topology)
        throws Exception {

    }

    @Override
    public void suspend(TopologyLayout topology, String asUser)
        throws Exception {

    }

    @Override
    public void resume(TopologyLayout topology, String asUser)
        throws Exception {

    }

    @Override
    public Status status(TopologyLayout topology, String asUser)
        throws Exception {
        return null;
    }

    @Override
    public LogLevelInformation configureLogLevel(TopologyLayout topology, LogLevel targetLogLevel,
        int durationSecs, String asUser)
        throws Exception {
        return null;
    }

    @Override
    public LogLevelInformation getLogLevel(TopologyLayout topology, String asUser)
        throws Exception {
        return null;
    }

    /**
     * the Path where any topology specific artifacts are kept
     */
    @Override
    public Path getArtifactsLocation(TopologyLayout topology) {
        return Paths.get(beamArtifactsLocation, generateBeamTopologyName(topology), "artifacts");
    }

    @Override
    public Path getExtraJarsLocation(TopologyLayout topology) {
        return Paths.get(beamArtifactsLocation, generateBeamTopologyName(topology), "jars");
    }

    @Override
    public String getRuntimeTopologyId(TopologyLayout topology, String asUser) {
        //TODO get the runtime topology id(calls cluster apis)
        Iterator<JobClusterMap> iterator = environmentService
            .getJobByTopologyName(topology.getName(), beamRunnerService.getName())
            .iterator();
        if (iterator.hasNext()) {
            return iterator.next().getJobId();
        }
        return null;
    }

 /*   private TopologyLayout copyTopologyLayout(TopologyLayout topology, TopologyDag replacedTopologyDag) {
        return new TopologyLayout(topology.getId(), topology.getName(), topology.getConfig(), replacedTopologyDag);
    }*/

    private List<String> getMavenArtifactsRelatedArgs(String mavenArtifacts) {
        List<String> args = new ArrayList<>();
        if (mavenArtifacts != null && !mavenArtifacts.isEmpty()) {
            args.add("--artifacts");
            args.add(mavenArtifacts);
            args.add("--artifactRepositories");
            args.add((String) conf.get("mavenRepoUrl"));

            String mavenLocalReposDir = (String) conf.get("mavenLocalRepositoryDirectory");
            if (StringUtils.isNotEmpty(mavenLocalReposDir)) {
                args.add("--mavenLocalRepositoryDirectory");
                args.add(mavenLocalReposDir);
            }

            String proxyUrl = (String) conf
                .get(com.hortonworks.streamline.common.Constants.CONFIG_HTTP_PROXY_URL);
            if (StringUtils.isNotEmpty(proxyUrl)) {
                args.add("--proxyUrl");
                args.add(proxyUrl);

                String proxyUsername = (String) conf
                    .get(com.hortonworks.streamline.common.Constants.CONFIG_HTTP_PROXY_USERNAME);
                String proxyPassword = (String) conf
                    .get(com.hortonworks.streamline.common.Constants.CONFIG_HTTP_PROXY_PASSWORD);
                if (proxyUsername != null && proxyPassword != null) {
                    // allow empty string but not null
                    args.add("--proxyUsername");
                    args.add(proxyUsername);
                    args.add("--proxyPassword");
                    args.add(proxyPassword);
                }
            }
        }
        return args;
    }





    /*private String createJaasFile(String kerberizedNimbusServiceName) {
        try {
            Path jaasFilePath = Files.createTempFile("jaas-", UUID.randomUUID().toString());

            String filePath = jaasFilePath.toAbsolutePath().toString();
            File jaasFile = null;
            return jaasFile.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Can't create JAAS file to connect to secure nimbus", e);
        }
    }*/



   /* private void createYamlFileForTest(TopologyLayout topology)
            throws Exception {
        createYamlFile(topology, false);
    }*/


    private void serializeTopologyDag(TopologyMapper topologyMapper, String filePath) {
        // Serialization
        try {
            //Saving of object in a file
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filePath));
            // Method for serialization of object
            out.writeObject(topologyMapper);
            out.close();

            System.out.println("Object has been serialized");

        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } finally {

        }
    }



    /*private void maybeAddNotifierPlugin(Config topologyConfig) {

    }

    private void registerEventLogger(Config topologyConfig) {
        topologyConfig.put(TOPOLOGY_EVENTLOGGER_REGISTER,
                Collections.singletonList(
                        Collections.singletonMap("class", TOPOLOGY_EVENTLOGGER_CLASSNAME_STREAMLINE))
        );
    }*/

    private String generateBeamTopologyName(TopologyLayout topology) {
        return BeamTopologyUtil.generateBeamTopologyName(topology.getId(), topology.getName());
    }

    private String getTopologyTempPath(String topologyName, String subDir) {
        return String.format(tempTopologyPath, topologyName, subDir);
    }


    private String getFilePath(TopologyLayout topology) {
        return this.beamArtifactsLocation + generateBeamTopologyName(topology) + ".yaml";
    }

    // Add topology level configs. catalogRootUrl, hbaseConf, hdfsConf,
    // numWorkers, etc.
   /* private void addTopologyConfig(Map<String, Object> yamlMap, Map<String, Object> topologyConfig) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(BeamTopologyLayoutConstants.YAML_KEY_CATALOG_ROOT_URL, catalogRootUrl);
        //Hack to work around HBaseBolt expecting this map in prepare method. Fix HBaseBolt and get rid of this. We use hbase-site.xml conf from ambari
        if (topologyConfig != null) {
            config.putAll(topologyConfig);
        }
        yamlMap.put(BeamTopologyLayoutConstants.YAML_KEY_CONFIG, config);
    }*/

    /*private void addComponentToCollection(Map<String, Object> yamlMap, Map<String, Object> yamlComponent, String collectionKey) {
        if (yamlComponent == null) {
            return;
        }

        List<Map<String, Object>> components = (List<Map<String, Object>>) yamlMap.get(collectionKey);
        if (components == null) {
            components = new ArrayList<>();
            yamlMap.put(collectionKey, components);
        }
        components.add(yamlComponent);
    }*/
}