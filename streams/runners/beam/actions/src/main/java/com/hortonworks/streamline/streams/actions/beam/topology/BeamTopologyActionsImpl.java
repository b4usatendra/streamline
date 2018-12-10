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
package com.hortonworks.streamline.streams.actions.beam.topology;

import com.google.common.base.*;
import com.hortonworks.streamline.streams.actions.*;
import com.hortonworks.streamline.streams.actions.topology.service.*;
import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import com.hortonworks.streamline.streams.beam.common.*;
import com.hortonworks.streamline.streams.catalog.*;
import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.service.*;
import com.hortonworks.streamline.streams.layout.*;
import com.hortonworks.streamline.streams.layout.beam.*;
import com.hortonworks.streamline.streams.layout.component.*;
import com.hortonworks.streamline.streams.layout.component.impl.testing.*;
import groovyx.net.http.*;
import org.apache.commons.io.*;
import org.apache.commons.lang.*;
import org.slf4j.*;

import java.io.*;
import java.io.File;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.*;

import static java.util.stream.Collectors.*;

/**
 * Storm implementation of the TopologyActions interface
 */
public class BeamTopologyActionsImpl implements TopologyActions {
    private static final Logger LOG = LoggerFactory.getLogger(BeamTopologyActionsImpl.class);
    /* public static final int DEFAULT_WAIT_TIME_SEC = 30;
     public static final int TEST_RUN_TOPOLOGY_DEFAULT_WAIT_MILLIS_FOR_SHUTDOWN = 30_000;
     public static final String ROOT_LOGGER_NAME = "ROOT";
     public static final String STREAMLINE_TOPOLOGY_CONFIG_CLUSTER_SECURITY_CONFIG = "clustersSecurityConfig";
     public static final String STREAMLINE_TOPOLOGY_CONFIG_CLUSTER_ID = "clusterId";
     public static final String STREAMLINE_TOPOLOGY_CONFIG_PRINCIPAL = "principal";
     public static final String STREAMLINE_TOPOLOGY_CONFIG_KEYTAB_PATH = "keytabPath";
     public static final String STORM_TOPOLOGY_CONFIG_AUTO_CREDENTIALS = "topology.auto-credentials";
     public static final String TOPOLOGY_CONFIG_KEY_CLUSTER_KEY_PREFIX_HDFS = "hdfs_";
     public static final String TOPOLOGY_CONFIG_KEY_CLUSTER_KEY_PREFIX_HBASE = "hbase_";
     public static final String TOPOLOGY_CONFIG_KEY_CLUSTER_KEY_PREFIX_HIVE = "hive_";
     public static final String TOPOLOGY_CONFIG_KEY_HDFS_KEYTAB_FILE = "hdfs.keytab.file";
     public static final String TOPOLOGY_CONFIG_KEY_HBASE_KEYTAB_FILE = "hbase.keytab.file";
     public static final String TOPOLOGY_CONFIG_KEY_HIVE_KEYTAB_FILE = "hive.keytab.file";
     public static final String TOPOLOGY_CONFIG_KEY_HDFS_KERBEROS_PRINCIPAL = "hdfs.kerberos.principal";
     public static final String TOPOLOGY_CONFIG_KEY_HBASE_KERBEROS_PRINCIPAL = "hbase.kerberos.principal";
     public static final String TOPOLOGY_CONFIG_KEY_HIVE_KERBEROS_PRINCIPAL = "hive.kerberos.principal";
     public static final String TOPOLOGY_CONFIG_KEY_HDFS_CREDENTIALS_CONFIG_KEYS = "hdfsCredentialsConfigKeys";
     public static final String TOPOLOGY_CONFIG_KEY_HBASE_CREDENTIALS_CONFIG_KEYS = "hbaseCredentialsConfigKeys";
     public static final String TOPOLOGY_CONFIG_KEY_HIVE_CREDENTIALS_CONFIG_KEYS = "hiveCredentialsConfigKeys";
     public static final String TOPOLOGY_CONFIG_KEY_NOTIFIER_PLUGIN = "storm.topology.submission.notifier.plugin.class";
     public static final String TOPOLOGY_AUTO_CREDENTIAL_CLASSNAME_HDFS = "org.apache.storm.hdfs.security.AutoHDFS";
     public static final String TOPOLOGY_AUTO_CREDENTIAL_CLASSNAME_HBASE = "org.apache.storm.hbase.security.AutoHBase";
     public static final String TOPOLOGY_AUTO_CREDENTIAL_CLASSNAME_HIVE = "org.apache.storm.hive.security.AutoHive";
     public static final String TOPOLOGY_NOTIFIER_PLUGIN_CLASSNAME_ATLAS = "org.apache.atlas.storm.hook.StormAtlasHook";
     public static final String TOPOLOGY_EVENTLOGGER_REGISTER = "topology.event.logger.register";
     public static final String TOPOLOGY_EVENTLOGGER_CLASSNAME_STREAMLINE = "com.hortonworks.streamline.streams.runtime.storm.event.sample.StreamlineEventLogger";
     private static final String DEFAULT_THRIFT_TRANSPORT_PLUGIN = "org.apache.storm.security.auth.SimpleTransportPlugin";
     private static final String DEFAULT_PRINCIPAL_TO_LOCAL = "org.apache.storm.security.auth.DefaultPrincipalToLocal";
     private static final String NON_SECURED_THRIFT_TRANSPORT_PLUGIN = "org.apache.storm.security.auth.SimpleTransportPlugin";
     private static final String NON_SECURED_PRINCIPAL_TO_LOCAL = "org.apache.storm.security.auth.DefaultPrincipalToLocal";
     private static final String NON_SECURED_NIMBUS_AUTHORIZER = "org.apache.storm.security.auth.authorizer.NoopAuthorizer";
     private static final String NON_SECURED_NIMBUS_IMPERSONATION_AUTHORIZER = "org.apache.storm.security.auth.authorizer.NoopAuthorizer";
     private static final String NIMBUS_SEEDS = "nimbus.seeds";
     private static final String NIMBUS_PORT = "nimbus.port";
     private static final Long DEFAULT_NIMBUS_THRIFT_MAX_BUFFER_SIZE = 1048576L;
     private String catalogRootUrl;
     private String nimbusSeeds;
     private Integer nimbusPort;
     private String thriftTransport;
     private String principalToLocal;
     private long nimbusThriftMaxBufferSize;

    */
    private String beamJarLocation;
    private Set<String> environmentServiceNames;

    private final ConcurrentHashMap<Long, Boolean> forceKillRequests = new ConcurrentHashMap<>();
    private HttpFileDownloader httpFileDownloader;
    private String stormCliPath = "beam";
    private String beamArtifactsLocation = "/beam-artifacts";

    private String javaJarCommand;


    private Map<String, Object> conf;
    private Optional<String> jaasFilePath;


    public BeamTopologyActionsImpl() {
    }

    @Override
    public void init(Map<String, Object> conf) {
        this.conf = conf;
        if (conf != null) {
            if (conf.containsKey(TopologyLayoutConstants.DEFAULT_ABSOLUTE_JAR_LOCATION_DIR)) {
                String defaultArtifactsLocation = (String) conf.get(TopologyLayoutConstants.DEFAULT_ABSOLUTE_JAR_LOCATION_DIR);
                beamArtifactsLocation = defaultArtifactsLocation + beamArtifactsLocation;
            }
		 /*
		 if (conf.containsKey(BeamTopologyLayoutConstants.STORM_HOME_DIR))
		 {
			String stormHomeDir = (String) conf.get(BeamTopologyLayoutConstants.STORM_HOME_DIR);
			if (!stormHomeDir.endsWith(File.separator))
			{
			   stormHomeDir += File.separator;
			}
			stormCliPath = stormHomeDir + "bin" + File.separator + "storm";
		 }

*/
            //catalogRootUrl = (String) conf.get(BeamTopologyLayoutConstants.YAML_KEY_CATALOG_ROOT_URL);

            this.beamJarLocation = (String) conf.get(BeamTopologyLayoutConstants.BEAM_PIPELINE_JAR_LOCATION_KEY);
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

		/* String stormApiRootUrl = (String) conf.get(TopologyLayoutConstants.STORM_API_ROOT_URL_KEY);
		 Subject subject = (Subject) conf.get(TopologyLayoutConstants.SUBJECT_OBJECT);

		 Client restClient = ClientBuilder.newClient(new ClientConfig());

		 this.client = new StormRestAPIClient(restClient, stormApiRootUrl, subject);
		 nimbusSeeds = (String) conf.get(NIMBUS_SEEDS);
		 nimbusPort = Integer.valueOf((String) conf.get(NIMBUS_PORT));

		 if (conf.containsKey(TopologyLayoutConstants.NIMBUS_THRIFT_MAX_BUFFER_SIZE))
		 {
			nimbusThriftMaxBufferSize = (Long) conf.get(TopologyLayoutConstants.NIMBUS_THRIFT_MAX_BUFFER_SIZE);
		 } else
		 {
			nimbusThriftMaxBufferSize = DEFAULT_NIMBUS_THRIFT_MAX_BUFFER_SIZE;
		 }
*/
            setupSecuredCluster(conf);

            EnvironmentService environmentService = (EnvironmentService) conf.get(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT);
            Number namespaceId = (Number) conf.get(TopologyLayoutConstants.NAMESPACE_ID);
		 /*this.serviceConfigurationReader = new AutoCredsServiceConfigurationReader(environmentService,
				 namespaceId.longValue());*/
            this.environmentServiceNames = environmentService.listServiceClusterMapping(namespaceId.longValue())
                    .stream()
                    .map(NamespaceServiceClusterMap::getServiceName)
                    .collect(Collectors.toSet());
        }

        try {
            httpFileDownloader = new HttpFileDownloader("tmp");
        } catch (Exception e) {
            e.printStackTrace();
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
    public void deploy(TopologyLayout topology, String mavenArtifacts, TopologyActionContext ctx, String asUser)
            throws Exception {

        //serialize topology
        String filePath = "/tmp/serialized_object";
        Map<String, Object> config = new HashMap<String, Object>();
        Set<Map.Entry<String, Object>> set = conf.entrySet();

        for (Map.Entry<String, Object> entry : set) {
            if (entry.getValue() instanceof String) {
                config.put(entry.getKey(), entry.getValue());
            }
        }
        serializeTopologyDag(new TopologyMapper(config, topology), filePath);

        ctx.setCurrentAction("Adding artifacts to jar");
        //downloadArtifactsAndCopyJars(mavenArtifacts, getExtraJarsLocation(topology));
        //Path jarToDeploy = addArtifactsToJar(getExtraJarsLocation(topology));
        Path jarToDeploy = Paths.get(beamJarLocation);

        ctx.setCurrentAction("Creating Beam Pipeline from topology");
        ctx.setCurrentAction("Deploying beam topology via 'java jar' command");
        List<String> commands = new ArrayList<String>();
        commands.add("java");

        commands.add("-jar");
        commands.add(jarToDeploy.toString());
        commands.add("-Dexec.args=\"--runner=DirectRunner\"");
        //commands.addAll(getExtraJarsArg(topology));
        //commands.addAll(getMavenArtifactsRelatedArgs(mavenArtifacts));
        //commands.add("--remote");

        LOG.info("Deploying Application {}", topology.getName());
        LOG.info(String.join(" ", commands));
        Process process = executeShellProcess(commands);
        ShellProcessResult shellProcessResult = waitProcessFor(process);
        /*
        int exitValue = shellProcessResult.exitValue;
        if (exitValue != 0) {
            LOG.error("Topology deploy command failed - exit code: {} / output: {}", exitValue, shellProcessResult.stdout);
            String[] lines = shellProcessResult.stdout.split("\\n");
            String errors = Arrays.stream(lines)
                    .filter(line -> line.startsWith("Exception") || line.startsWith("Caused by"))
                    .collect(Collectors.joining(", "));
            Pattern pattern = Pattern.compile("Topology with name `(.*)` already exists on cluster");
            Matcher matcher = pattern.matcher(errors);
            if (matcher.find()) {
                throw new TopologyAlreadyExistsOnCluster(matcher.group(1));
            } else {
                throw new Exception("Topology could not be deployed successfully: storm deploy command failed with " + errors);
            }
        }*/

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

    private ShellProcessResult waitProcessFor(Process process) throws IOException, InterruptedException {
        StringWriter sw = new StringWriter();
        while (process.isAlive()) {
            try {
                final BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream()));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
                reader.close();
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
        IOUtils.copy(process.getInputStream(), sw, Charset.defaultCharset());
        String stdout = sw.toString();
        //process.waitFor();
        int exitValue = process.exitValue();
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
            for (String dependency : mavenDependencies) {
                if (!isDownloadedSet.contains(dependency)) {
                    String[] dependencyArray = dependency.split(":");
                    try {
                        String downloadUrl = ArtifactoryJarPathResolver.resolve(url, dependencyArray[0], dependencyArray[1], dependencyArray[2]);
                        Path path = httpFileDownloader.download(downloadUrl);
                        FileUtils.copyFileToDirectory(new File(path.toAbsolutePath().toString()), new File(destinationPath.toAbsolutePath().toString()));
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
            LOG.debug("Artifacts directory {} does not exist, not adding any artifacts to jar", artifactsLocation);
        }
        return jarFile;
    }

    @Override
    public void runTest(TopologyLayout topology, TopologyTestRunHistory testRunHistory, String mavenArtifacts,
                        Map<String, TestRunSource> testRunSourcesForEachSource,
                        Map<String, TestRunProcessor> testRunProcessorsForEachProcessor,
                        Map<String, TestRunRulesProcessor> testRunRulesProcessorsForEachProcessor,
                        Map<String, TestRunSink> testRunSinksForEachSink, Optional<Long> durationSecs)
            throws Exception {
        TopologyDag originalTopologyDag = topology.getTopologyDag();

        TestTopologyDagCreatingVisitor visitor = new TestTopologyDagCreatingVisitor(originalTopologyDag,
                testRunSourcesForEachSource, testRunProcessorsForEachProcessor, testRunRulesProcessorsForEachProcessor,
                testRunSinksForEachSink);
        originalTopologyDag.traverse(visitor);
    }

    @Override
    public boolean killTest(TopologyTestRunHistory testRunHistory) {
        // just turn on the flag only if it exists
        LOG.info("Turning on force kill flag on test run history {}", testRunHistory.getId());
        Boolean newValue = forceKillRequests.computeIfPresent(testRunHistory.getId(), (id, flag) -> true);
        return newValue != null;
    }

    @Override
    public void kill(TopologyLayout topology, String asUser)
            throws Exception {

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
    public LogLevelInformation configureLogLevel(TopologyLayout topology, LogLevel targetLogLevel, int durationSecs, String asUser)
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

            String proxyUrl = (String) conf.get(com.hortonworks.streamline.common.Constants.CONFIG_HTTP_PROXY_URL);
            if (StringUtils.isNotEmpty(proxyUrl)) {
                args.add("--proxyUrl");
                args.add(proxyUrl);

                String proxyUsername = (String) conf.get(com.hortonworks.streamline.common.Constants.CONFIG_HTTP_PROXY_USERNAME);
                String proxyPassword = (String) conf.get(com.hortonworks.streamline.common.Constants.CONFIG_HTTP_PROXY_PASSWORD);
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
        return BeamTopologyUtil.generateStormTopologyName(topology.getId(), topology.getName());
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
