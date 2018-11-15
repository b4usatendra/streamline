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

import com.hortonworks.streamline.common.*;
import com.hortonworks.streamline.streams.actions.*;
import com.hortonworks.streamline.streams.beam.common.*;
import com.hortonworks.streamline.streams.catalog.*;
import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.service.*;
import com.hortonworks.streamline.streams.layout.*;
import com.hortonworks.streamline.streams.layout.beam.*;
import com.hortonworks.streamline.streams.layout.component.*;
import com.hortonworks.streamline.streams.layout.component.impl.testing.*;
import org.apache.beam.sdk.*;
import org.apache.commons.lang.*;
import org.slf4j.*;

import java.io.File;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * Storm implementation of the TopologyActions interface
 */
public class BeamTopologyActionsImpl implements TopologyActions
{
   private static final Logger LOG = LoggerFactory.getLogger(BeamTopologyActionsImpl.class);
   public static final int DEFAULT_WAIT_TIME_SEC = 30;
   public static final int TEST_RUN_TOPOLOGY_DEFAULT_WAIT_MILLIS_FOR_SHUTDOWN = 30_000;
   public static final String ROOT_LOGGER_NAME = "ROOT";

   private static final String DEFAULT_THRIFT_TRANSPORT_PLUGIN = "org.apache.storm.security.auth.SimpleTransportPlugin";
   private static final String DEFAULT_PRINCIPAL_TO_LOCAL = "org.apache.storm.security.auth.DefaultPrincipalToLocal";

   private static final String NON_SECURED_THRIFT_TRANSPORT_PLUGIN = "org.apache.storm.security.auth.SimpleTransportPlugin";
   private static final String NON_SECURED_PRINCIPAL_TO_LOCAL = "org.apache.storm.security.auth.DefaultPrincipalToLocal";
   private static final String NON_SECURED_NIMBUS_AUTHORIZER = "org.apache.storm.security.auth.authorizer.NoopAuthorizer";
   private static final String NON_SECURED_NIMBUS_IMPERSONATION_AUTHORIZER = "org.apache.storm.security.auth.authorizer.NoopAuthorizer";

   private static final String NIMBUS_SEEDS = "nimbus.seeds";
   private static final String NIMBUS_PORT = "nimbus.port";

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

   private static final Long DEFAULT_NIMBUS_THRIFT_MAX_BUFFER_SIZE = 1048576L;

   public static final String TOPOLOGY_EVENTLOGGER_REGISTER = "topology.event.logger.register";
   public static final String TOPOLOGY_EVENTLOGGER_CLASSNAME_STREAMLINE = "com.hortonworks.streamline.streams.runtime.storm.event.sample.StreamlineEventLogger";

   private String stormArtifactsLocation = "/tmp/storm-artifacts/";
   private String stormCliPath = "storm";
   private String stormJarLocation;
   private String catalogRootUrl;
   private String javaJarCommand;
   private String nimbusSeeds;
   private Integer nimbusPort;
   private Map<String, Object> conf;

   private String thriftTransport;
   private Optional<String> jaasFilePath;
   private String principalToLocal;
   private long nimbusThriftMaxBufferSize;

   private final ConcurrentHashMap<Long, Boolean> forceKillRequests = new ConcurrentHashMap<>();
   private Set<String> environmentServiceNames;

   public BeamTopologyActionsImpl()
   {
   }

   @Override
   public void init(Map<String, Object> conf)
   {
	  this.conf = conf;
	  if (conf != null)
	  {
		 /*if (conf.containsKey(BeamTopologyLayoutConstants.STORM_ARTIFACTS_LOCATION_KEY))
		 {
			stormArtifactsLocation = (String) conf.get(BeamTopologyLayoutConstants.STORM_ARTIFACTS_LOCATION_KEY);
		 }
		 if (conf.containsKey(BeamTopologyLayoutConstants.STORM_HOME_DIR))
		 {
			String stormHomeDir = (String) conf.get(BeamTopologyLayoutConstants.STORM_HOME_DIR);
			if (!stormHomeDir.endsWith(File.separator))
			{
			   stormHomeDir += File.separator;
			}
			stormCliPath = stormHomeDir + "bin" + File.separator + "storm";
		 }
		 this.stormJarLocation = (String) conf.get(BeamTopologyLayoutConstants.STORM_JAR_LOCATION_KEY);
*/
		 catalogRootUrl = (String) conf.get(BeamTopologyLayoutConstants.YAML_KEY_CATALOG_ROOT_URL);

		 Map<String, String> env = System.getenv();
		 String javaHomeStr = env.get("JAVA_HOME");
		 if (StringUtils.isNotEmpty(javaHomeStr))
		 {
			if (!javaHomeStr.endsWith(File.separator))
			{
			   javaHomeStr += File.separator;
			}
			javaJarCommand = javaHomeStr + "bin" + File.separator + "jar";
		 } else
		 {
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
		 setupSecuredStormCluster(conf);

		 EnvironmentService environmentService = (EnvironmentService) conf.get(TopologyLayoutConstants.ENVIRONMENT_SERVICE_OBJECT);
		 Number namespaceId = (Number) conf.get(TopologyLayoutConstants.NAMESPACE_ID);
		 /*this.serviceConfigurationReader = new AutoCredsServiceConfigurationReader(environmentService,
				 namespaceId.longValue());*/
		 this.environmentServiceNames = environmentService.listServiceClusterMapping(namespaceId.longValue())
				 .stream()
				 .map(NamespaceServiceClusterMap::getServiceName)
				 .collect(Collectors.toSet());
	  }
	  File f = new File(stormArtifactsLocation);
	  if (!f.exists() && !f.mkdirs())
	  {
		 throw new RuntimeException("Could not create directory " + f.getAbsolutePath());
	  }
   }

   private void setupSecuredStormCluster(Map<String, Object> conf)
   {
	  thriftTransport = (String) conf.get(TopologyLayoutConstants.STORM_THRIFT_TRANSPORT);

	  if (conf.containsKey(TopologyLayoutConstants.STORM_NIMBUS_PRINCIPAL_NAME))
	  {
		 String nimbusPrincipal = (String) conf.get(TopologyLayoutConstants.STORM_NIMBUS_PRINCIPAL_NAME);
		 String kerberizedNimbusServiceName = nimbusPrincipal.split("/")[0];
		 jaasFilePath = Optional.of(createJaasFile(kerberizedNimbusServiceName));
	  } else
	  {
		 jaasFilePath = Optional.empty();
	  }

	  principalToLocal = (String) conf.getOrDefault(TopologyLayoutConstants.STORM_PRINCIPAL_TO_LOCAL, DEFAULT_PRINCIPAL_TO_LOCAL);

	  if (thriftTransport == null)
	  {
		 if (jaasFilePath.isPresent())
		 {
			thriftTransport = (String) conf.get(TopologyLayoutConstants.STORM_SECURED_THRIFT_TRANSPORT);
		 } else
		 {
			thriftTransport = (String) conf.get(TopologyLayoutConstants.STORM_NONSECURED_THRIFT_TRANSPORT);
		 }
	  }

	  // if it's still null, set to default
	  if (thriftTransport == null)
	  {
		 thriftTransport = DEFAULT_THRIFT_TRANSPORT_PLUGIN;
	  }
   }

   @Override
   public void deploy(TopologyLayout topology, String mavenArtifacts, TopologyActionContext ctx, String asUser)
		   throws Exception
   {

   }

   @Override
   public void runTest(TopologyLayout topology, TopologyTestRunHistory testRunHistory, String mavenArtifacts,
		   Map<String, TestRunSource> testRunSourcesForEachSource,
		   Map<String, TestRunProcessor> testRunProcessorsForEachProcessor,
		   Map<String, TestRunRulesProcessor> testRunRulesProcessorsForEachProcessor,
		   Map<String, TestRunSink> testRunSinksForEachSink, Optional<Long> durationSecs)
		   throws Exception
   {
	  TopologyDag originalTopologyDag = topology.getTopologyDag();

	  TestTopologyDagCreatingVisitor visitor = new TestTopologyDagCreatingVisitor(originalTopologyDag,
			  testRunSourcesForEachSource, testRunProcessorsForEachProcessor, testRunRulesProcessorsForEachProcessor,
			  testRunSinksForEachSink);
	  originalTopologyDag.traverse(visitor);
	  TopologyDag testTopologyDag = visitor.getTestTopologyDag();

	  TopologyLayout testTopology = copyTopologyLayout(topology, testTopologyDag);
   }

   @Override
   public boolean killTest(TopologyTestRunHistory testRunHistory)
   {
	  // just turn on the flag only if it exists
	  LOG.info("Turning on force kill flag on test run history {}", testRunHistory.getId());
	  Boolean newValue = forceKillRequests.computeIfPresent(testRunHistory.getId(), (id, flag) -> true);
	  return newValue != null;
   }

   @Override
   public void kill(TopologyLayout topology, String asUser)
		   throws Exception
   {

   }

   @Override
   public void validate(TopologyLayout topology)
		   throws Exception
   {

   }

   @Override
   public void suspend(TopologyLayout topology, String asUser)
		   throws Exception
   {

   }

   @Override
   public void resume(TopologyLayout topology, String asUser)
		   throws Exception
   {

   }

   @Override
   public Status status(TopologyLayout topology, String asUser)
		   throws Exception
   {
	  return null;
   }

   @Override
   public LogLevelInformation configureLogLevel(TopologyLayout topology, LogLevel targetLogLevel, int durationSecs, String asUser)
		   throws Exception
   {
	  return null;
   }

   @Override
   public LogLevelInformation getLogLevel(TopologyLayout topology, String asUser)
		   throws Exception
   {
	  return null;
   }

   /**
	* the Path where any topology specific artifacts are kept
	*/
   @Override
   public Path getArtifactsLocation(TopologyLayout topology)
   {
	  return Paths.get(stormArtifactsLocation, generateStormTopologyName(topology), "artifacts");
   }

   @Override
   public Path getExtraJarsLocation(TopologyLayout topology)
   {
	  return Paths.get(stormArtifactsLocation, generateStormTopologyName(topology), "jars");
   }

   @Override
   public String getRuntimeTopologyId(TopologyLayout topology, String asUser)
   {
      //TODO get the runtime topology id(calls cluster apis)
      return null;
   }

   private TopologyLayout copyTopologyLayout(TopologyLayout topology, TopologyDag replacedTopologyDag)
   {
	  return new TopologyLayout(topology.getId(), topology.getName(), topology.getConfig(), replacedTopologyDag);
   }

   private List<String> getMavenArtifactsRelatedArgs(String mavenArtifacts)
   {
	  List<String> args = new ArrayList<>();
	  if (mavenArtifacts != null && !mavenArtifacts.isEmpty())
	  {
		 args.add("--artifacts");
		 args.add(mavenArtifacts);
		 args.add("--artifactRepositories");
		 args.add((String) conf.get("mavenRepoUrl"));

		 String mavenLocalReposDir = (String) conf.get("mavenLocalRepositoryDirectory");
		 if (StringUtils.isNotEmpty(mavenLocalReposDir))
		 {
			args.add("--mavenLocalRepositoryDirectory");
			args.add(mavenLocalReposDir);
		 }

		 String proxyUrl = (String) conf.get(com.hortonworks.streamline.common.Constants.CONFIG_HTTP_PROXY_URL);
		 if (StringUtils.isNotEmpty(proxyUrl))
		 {
			args.add("--proxyUrl");
			args.add(proxyUrl);

			String proxyUsername = (String) conf.get(com.hortonworks.streamline.common.Constants.CONFIG_HTTP_PROXY_USERNAME);
			String proxyPassword = (String) conf.get(com.hortonworks.streamline.common.Constants.CONFIG_HTTP_PROXY_PASSWORD);
			if (proxyUsername != null && proxyPassword != null)
			{
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

   private List<String> getTempWorkerArtifactArgs()
		   throws IOException
   {
	  List<String> args = new ArrayList<>();

	  Path tempArtifacts = Files.createTempDirectory("worker-artifacts-");
	  args.add("-c");
	  args.add("storm.workers.artifacts.dir=" + tempArtifacts.toFile().getAbsolutePath());

	  return args;
   }

   private List<String> getNoMetricConsumerArgs()
   {
	  List<String> args = new ArrayList<>();

	  args.add("-c");
	  args.add("topology.metrics.consumer.register=[]");
	  args.add("-c");
	  args.add("storm.cluster.metrics.consumer.register=[]");

	  return args;
   }

   private String createJaasFile(String kerberizedNimbusServiceName)
   {
	  try
	  {
		 Path jaasFilePath = Files.createTempFile("jaas-", UUID.randomUUID().toString());

		 String filePath = jaasFilePath.toAbsolutePath().toString();
		 File jaasFile = null;
		 return jaasFile.getAbsolutePath();
	  }
	  catch (IOException e)
	  {
		 throw new RuntimeException("Can't create JAAS file to connect to secure nimbus", e);
	  }
   }

   private void createYamlFileForDeploy(TopologyLayout topology)
		   throws Exception
   {
	  createYamlFile(topology, true);
   }

   private void createYamlFileForTest(TopologyLayout topology)
		   throws Exception
   {
	  createYamlFile(topology, false);
   }

   private void createYamlFile(TopologyLayout topology, boolean deploy)
		   throws Exception
   {
	  Map<String, Object> yamlMap;
	  File f;
	  OutputStreamWriter fileWriter = null;
	  try
	  {
		 f = new File(this.getFilePath(topology));
		 if (f.exists())
		 {
			if (!f.delete())
			{
			   throw new Exception("Unable to delete old storm " +
					   "artifact for topology id " + topology.getId());
			}
		 }
		 yamlMap = new LinkedHashMap<>();
		 yamlMap.put(BeamTopologyLayoutConstants.YAML_KEY_NAME, generateStormTopologyName(topology));
		 TopologyDag topologyDag = topology.getTopologyDag();
		 LOG.debug("Initial Topology config {}", topology.getConfig());
		 BeamTopologyFluxGenerator fluxGenerator = new BeamTopologyFluxGenerator(topology, conf,
				 getExtraJarsLocation(topology));
		 topologyDag.traverse(fluxGenerator);
		 Pipeline pipeline = fluxGenerator.getPipeline();
		 pipeline.run();
		/* for (Map.Entry<String, Map<String, Object>> entry : fluxGenerator.getYamlKeysAndComponents())
		 {
			addComponentToCollection(yamlMap, entry.getValue(), entry.getKey());
		 }
		 Config topologyConfig = fluxGenerator.getTopologyConfig();
		 registerEventLogger(topologyConfig);
		 maybeAddNotifierPlugin(topologyConfig);
		 Map<String, Object> properties = topologyConfig.getProperties();
		 if (!deploy)
		 {
			LOG.debug("Disabling topology event logger for test mode...");
			properties.put("topology.eventlogger.executors", 0);
		 }

		 LOG.debug("Final Topology properties {}", properties);

		 addTopologyConfig(yamlMap, properties);
		 DumperOptions options = new DumperOptions();
		 options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
		 options.setSplitLines(false);
		 //options.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
		 Yaml yaml = new Yaml(options);
		 fileWriter = new OutputStreamWriter(new FileOutputStream(f), "UTF-8");
		 yaml.dump(yamlMap, fileWriter);
		 return f.getAbsolutePath();*/
	  }
	  finally
	  {
		 if (fileWriter != null)
		 {
			fileWriter.close();
		 }
	  }
   }

   private void maybeAddNotifierPlugin(Config topologyConfig)
   {

   }

   private void registerEventLogger(Config topologyConfig)
   {
	  topologyConfig.put(TOPOLOGY_EVENTLOGGER_REGISTER,
			  Collections.singletonList(
					  Collections.singletonMap("class", TOPOLOGY_EVENTLOGGER_CLASSNAME_STREAMLINE))
	  );
   }

   private String generateStormTopologyName(TopologyLayout topology)
   {
	  return BeamTopologyUtil.generateStormTopologyName(topology.getId(), topology.getName());
   }

   private String getFilePath(TopologyLayout topology)
   {
	  return this.stormArtifactsLocation + generateStormTopologyName(topology) + ".yaml";
   }

   // Add topology level configs. catalogRootUrl, hbaseConf, hdfsConf,
   // numWorkers, etc.
   private void addTopologyConfig(Map<String, Object> yamlMap, Map<String, Object> topologyConfig)
   {
	  Map<String, Object> config = new LinkedHashMap<>();
	  config.put(BeamTopologyLayoutConstants.YAML_KEY_CATALOG_ROOT_URL, catalogRootUrl);
	  //Hack to work around HBaseBolt expecting this map in prepare method. Fix HBaseBolt and get rid of this. We use hbase-site.xml conf from ambari
	  if (topologyConfig != null)
	  {
		 config.putAll(topologyConfig);
	  }
	  yamlMap.put(BeamTopologyLayoutConstants.YAML_KEY_CONFIG, config);
   }

   private void addComponentToCollection(Map<String, Object> yamlMap, Map<String, Object> yamlComponent, String collectionKey)
   {
	  if (yamlComponent == null)
	  {
		 return;
	  }

	  List<Map<String, Object>> components = (List<Map<String, Object>>) yamlMap.get(collectionKey);
	  if (components == null)
	  {
		 components = new ArrayList<>();
		 yamlMap.put(collectionKey, components);
	  }
	  components.add(yamlComponent);
   }
}
