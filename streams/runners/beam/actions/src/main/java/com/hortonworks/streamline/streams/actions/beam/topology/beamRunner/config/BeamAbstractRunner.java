package com.hortonworks.streamline.streams.actions.beam.topology.beamRunner.config;

import com.google.common.base.Joiner;
import com.hortonworks.streamline.streams.actions.TopologyActionContext;
import com.hortonworks.streamline.streams.actions.TopologyActions;
import com.hortonworks.streamline.streams.actions.beam.topology.BeamServiceConfigurationReader;
import com.hortonworks.streamline.streams.actions.beam.topology.BeamTopologyActionUtils;
import com.hortonworks.streamline.streams.actions.beam.topology.TestTopologyDagCreatingVisitor;
import com.hortonworks.streamline.streams.actions.topology.service.ArtifactoryJarPathResolver;
import com.hortonworks.streamline.streams.actions.topology.service.HttpFileDownloader;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyUtil;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Satendra Sahu on 4/23/19
 */
public abstract class BeamAbstractRunner implements TopologyActions {

    private static final Logger LOG = LoggerFactory.getLogger(BeamAbstractRunner.class);

    protected String beamJarLocation;
    private final ConcurrentHashMap<Long, Boolean> forceKillRequests = new ConcurrentHashMap<>();
    protected HttpFileDownloader httpFileDownloader;
    protected String beamArtifactsLocation = BeamTopologyLayoutConstants.DEFAULT_BEAM_ARTIFACTS_LOCATION;
    protected String javaJarCommand;
    protected BeamServiceConfigurationReader serviceConfigurationReader;
    protected Map<String, Object> conf;
    protected EnvironmentService environmentService;
    protected Service beamRunnerService;
    protected Map<String, String> beamRunnerConfig;

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

    @Override
    public abstract void deploy(TopologyLayout topology, String mavenArtifacts,
        TopologyActionContext ctx,
        String asUser) throws Exception;


    @Override
    public void runTest(TopologyLayout topology, TopologyTestRunHistory testRunHistory,
        String mavenArtifacts, Map<String, TestRunSource> testRunSourcesForEachSource,
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
    public abstract boolean killTest(TopologyTestRunHistory testRunHistory);

    @Override
    public abstract void kill(TopologyLayout topology, String asUser) throws Exception;

    @Override
    public void validate(TopologyLayout topology) throws Exception {

    }

    @Override
    public abstract void suspend(TopologyLayout topology, String asUser) throws Exception;

    @Override
    public abstract void resume(TopologyLayout topology, String asUser) throws Exception;

    @Override
    public abstract Status status(TopologyLayout topology, String asUser) throws Exception;

    @Override
    public LogLevelInformation configureLogLevel(TopologyLayout topology, LogLevel targetLogLevel,
        int durationSecs, String asUser) throws Exception {
        return null;
    }

    @Override
    public LogLevelInformation getLogLevel(TopologyLayout topology, String asUser)
        throws Exception {
        return null;
    }

    /**
     * the Path where topology specific artifacts are kept
     */
    @Override
    public Path getArtifactsLocation(TopologyLayout topology) {
        return Paths.get(beamArtifactsLocation, generateBeamTopologyName(topology), "artifacts");
    }

    /**
     * the Path where extra jars to be deployed are kept
     */
    @Override
    public Path getExtraJarsLocation(TopologyLayout topology) {
        return Paths.get(beamArtifactsLocation, generateBeamTopologyName(topology), "jars");
    }

    /**
     * the topology id which is running in runtime streaming engine
     */
    @Override
    public String getRuntimeTopologyId(TopologyLayout topology, String asUser) {
        return null;
    }

    protected String generateBeamTopologyName(TopologyLayout topology) {
        return BeamTopologyUtil.generateBeamTopologyName(topology.getId(), topology.getName());
    }

    protected String getTopologyTempPath(String topologyName, String subDir) {
        return String.format(BeamTopologyLayoutConstants.TEMP_TOPLOGY_PATH, topologyName, subDir);
    }


    protected String getFilePath(TopologyLayout topology) {
        return this.beamArtifactsLocation + generateBeamTopologyName(topology) + ".yaml";
    }


    protected Path addArtifactsToJar(Path artifactsLocation) throws Exception {
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


       /*private void maybeAddNotifierPlugin(Config topologyConfig) {

    }

    private void registerEventLogger(Config topologyConfig) {
        topologyConfig.put(TOPOLOGY_EVENTLOGGER_REGISTER,
                Collections.singletonList(
                        Collections.singletonMap("class", TOPOLOGY_EVENTLOGGER_CLASSNAME_STREAMLINE))
        );
    }*/

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


    protected void serializeTopologyDag(TopologyMapper topologyMapper, String filePath) {
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

    protected List<String> getMavenArtifactsRelatedArgs(String mavenArtifacts) {
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

    protected static class ShellProcessResult {

        public final int exitValue;
        public final String stdout;

        ShellProcessResult(int exitValue, String stdout) {
            this.exitValue = exitValue;
            this.stdout = stdout;
        }
    }

    protected Process executeShellProcess(List<String> commands) throws Exception {
        LOG.info("Executing command: {}", Joiner.on(" ").join(commands));
        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        processBuilder.redirectErrorStream(true);
        return processBuilder.start();
    }

    protected ShellProcessResult waitProcessFor(Process process)
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


    protected void downloadArtifactsAndCopyJars(String mavenDeps, Path destinationPath) {
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

    protected List<String> getExtraJarsArg(TopologyLayout topology) {
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

    protected void updateJobClusterMapping(String stdout, JobClusterMap jobClusterMap) {
        String[] output = stdout.split("\\n\\t");

        if (!beamRunnerConfig.containsKey(BeamTopologyLayoutConstants.FLINK_MASTER_KEY)) {
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

    protected void addJaaSConfig(String topologyName)
        throws Exception {
        BeamTopologyActionUtils.validateSSLConfig(conf);
        File topologyMetaDir = new File(getTopologyTempPath(topologyName, "jaas"));
        topologyMetaDir.mkdirs();
        this.conf.put("jaas_conf", topologyMetaDir.toString());
        /*List<String> commands = new ArrayList<String>();

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
        }*/

    }

}
