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

import static java.util.stream.Collectors.toList;

import com.google.common.base.Joiner;
import com.hortonworks.streamline.streams.actions.TopologyActionContext;
import com.hortonworks.streamline.streams.actions.TopologyActions;
import com.hortonworks.streamline.streams.actions.topology.service.ArtifactoryJarPathResolver;
import com.hortonworks.streamline.streams.actions.topology.service.HttpFileDownloader;
import com.hortonworks.streamline.streams.beam.common.BeamRunner;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyUtil;
import com.hortonworks.streamline.streams.beam.common.FlinkRestAPIClient;
import com.hortonworks.streamline.streams.catalog.TopologyTestRunHistory;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
  private static final String TOPOLOGY_SERIALIZED_OBJECT_PATH = "/serializedObject";
  private FlinkRestAPIClient flinkClient;
  private String beamJarLocation;
  private final ConcurrentHashMap<Long, Boolean> forceKillRequests = new ConcurrentHashMap<>();
  private HttpFileDownloader httpFileDownloader;
  private String beamArtifactsLocation = DEFAULT_BEAM_ARTIFACTS_LOCATION;
  private final String tempTopologyPath = "/tmp/topology/%s/%s/";
  private String javaJarCommand;
  private BeamServiceConfigurationReader serviceConfigurationReader;
  private Map<String, Object> conf;
  private Optional<String> jaasFilePath;

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

      EnvironmentService environmentService = (EnvironmentService) conf
          .get(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT);
      Number namespaceId = (Number) conf.get(TopologyLayoutConstants.NAMESPACE_ID);

      serviceConfigurationReader = new BeamServiceConfigurationReader(environmentService,
          namespaceId.longValue());

      Map<String, String> runnerConfig = serviceConfigurationReader.getRunnerConfig();
      if (!runnerConfig.containsKey(FLINK_MASTER_KEY)) {
        throw new RuntimeException(String.format("%s config not found", FLINK_MASTER_KEY));
      }
      flinkClient = new FlinkRestAPIClient(
          String.format("http://%s", runnerConfig.get(FLINK_MASTER_KEY)),
          (Subject) conf.get(TopologyLayoutConstants.SUBJECT_OBJECT),
          ClientBuilder.newClient(new ClientConfig()));

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


/*
        if (conf.containsKey(TopologyLayoutConstants.STORM_NIMBUS_PRINCIPAL_NAME)) {
            String nimbusPrincipal = (String) conf.get(BeamTopologyLayoutConstants.PRINCIPAL_NAME);
            String kerberizedNimbusServiceName = nimbusPrincipal.split("/")[0];
            jaasFilePath = Optional.of(createJaasFile(kerberizedNimbusServiceName));
        } else {
            jaasFilePath = Optional.empty();
        }

        principalToLocal = (String) conf.getOrDefault(TopologyLayoutConstants.STORM_PRINCIPAL_TO_LOCAL, DEFAULT_PRINCIPAL_TO_LOCAL);

        if (thriftTransport == null) {
            if (jaasFilePath.isPresent()) {
                thriftTransport = (String) conf.get(TopologyLayoutConstants.STORM_SECURED_THRIFT_TRANSPORT);
            } else {
                thriftTransport = (String) conf.get(TopologyLayoutConstants.STORM_NONSECURED_THRIFT_TRANSPORT);
            }
        }

        // if it's still null, set to default
        if (thriftTransport == null) {
            thriftTransport = DEFAULT_THRIFT_TRANSPORT_PLUGIN;
        }*/
  }

  @Override
  public void deploy(TopologyLayout topology, String mavenArtifacts, TopologyActionContext ctx,
      String asUser)
      throws Exception {

    //Preconditions before deploying
    BeamRunner runner = getRunnerArg();
    String runnerMasterCmd = getRunnerMaster(runner);

    Map<String, Object> config = new HashMap<String, Object>();
    File file = new File(getTopologyTempPath(topology.getName(), "jaas"));
    file.mkdirs();
    conf.put(BeamTopologyLayoutConstants.JAAS_CONF_PATH, file.getAbsolutePath() + "/jaas.conf");

    //serialize topology
    file = new File(getTopologyTempPath(topology.getName(), "serialization"));
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
    downloadArtifactsAndCopyJars(mavenArtifacts, getExtraJarsLocation(topology));
    Path jarToDeploy = addArtifactsToJar(getExtraJarsLocation(topology));

    ctx.setCurrentAction("Creating Flink Pipeline from topology");
    ctx.setCurrentAction("Deploying beam topology via 'java jar' command");
    List<String> commands = new ArrayList<String>();
    commands.add("java");

    commands.add("-jar");
    commands.add(jarToDeploy.toString());
    commands.add(filePath);
    commands.add("--runner=" + runner);
    commands.add(runnerMasterCmd);

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
    }
  }

  private BeamRunner getRunnerArg() {
    String runner = serviceConfigurationReader.getStreamingEngine();
    switch (runner.toLowerCase()) {
      case "flink":
        return BeamRunner.FlinkRunner;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported runner: %s", runner));
    }
  }

  private String getRunnerMaster(BeamRunner runner) {
    Map<String, String> runnerConfig = serviceConfigurationReader.getRunnerConfig();
    switch (runner) {
      case FlinkRunner:
        if (!runnerConfig.containsKey(FLINK_MASTER_KEY)) {
          throw new RuntimeException(String.format("%s config not found", FLINK_MASTER_KEY));
        }
        return new StringBuffer("--flinkMaster=").append(runnerConfig.get(FLINK_MASTER_KEY))
            .toString();
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
    LOG.debug("Executing command: {}", Joiner.on(" ").join(commands));
    ProcessBuilder processBuilder = new ProcessBuilder(commands);
    processBuilder.redirectErrorStream(true);
    return processBuilder.start();
  }

  private ShellProcessResult waitProcessFor(Process process)
      throws IOException, InterruptedException {
    StringWriter sw = new StringWriter();
        /*while (process.isAlive()) {
            final BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null) {

            }
            reader.close();
        }*/
    int exitValue = 0;
    process.waitFor(10, TimeUnit.SECONDS);
    if (!process.isAlive() && process.exitValue() == -1) {
      exitValue = -1;
      IOUtils.copy(process.getInputStream(), sw, Charset.defaultCharset());
    } else {
      final BufferedReader reader = new BufferedReader(
          new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        LOG.info(line);
        sw.append(line);
        if (line.contains("Submitting job")) {
          break;
        }
      }


            /*int len = process.getInputStream().read(buffer);
            byte[] buffer = new byte[2048];
            while (len!=-1){
                log = IOUtils.toString(buffer,Charset.defaultCharset());
                IOUtils.buffer(buffer,sw,Charset.defaultCharset());
                if(stopStreamCopy)
                    break;
                if(sw.toString().toLowerCase().contains("Submitting job"))
                    stopStreamCopy = true;
                len = process.getInputStream().read(buffer);
            }
            LOG.info(sw.toString());*/
    }
    //IOUtils.copy(process.getInputStream(), sw, Charset.defaultCharset());
    String stdout = sw.toString();
    LOG.debug("Command output: {}", stdout);
    LOG.debug("Command exit status: {}", exitValue);
    return new ShellProcessResult(exitValue, stdout);
  }


  private void downloadArtifactsAndCopyJars(String mavenDeps, Path destinationPath) {
    String[] mavenDependencies = mavenDeps.split(",");
    String mavenRepo = (String) conf.get("mavenRepoUrl");
    String[] mavenRepositories = mavenRepo.split(",");
    Set<String> isDownloadedSet = new HashSet<>();
    for (String url : mavenRepositories) {
      url = url.split("\\^")[1];
      ArtifactoryJarPathResolver artifactoryJarPathResolver = new ArtifactoryJarPathResolver(url);
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
            LOG.error("%s not found in repository %s", dependency, url);
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
      LOG.debug("Extra jars directory {} does not exist, not adding any extra jars", extraJarsPath);
    }
    if (!jars.isEmpty()) {
      args.add("--jars");
      args.add(Joiner.on(",").join(jars));
    }
    return args;
  }

  private Path addArtifactsToJar(Path artifactsLocation) throws Exception {
    Path jarFile = Paths.get(beamJarLocation);
    if (artifactsLocation.toFile().isDirectory()) {
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
        ShellProcessResult shellProcessResult = waitProcessFor(process);
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
    }
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

    TestTopologyDagCreatingVisitor visitor = new TestTopologyDagCreatingVisitor(originalTopologyDag,
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
    String jobId = flinkClient.getJobId(topology.getName());
    if (jobId == null) {
      throw new NotFoundException(String.format("Topology [%s] not found", topology.getName()));
    }
    flinkClient.stopJob(jobId);
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