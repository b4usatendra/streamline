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
package com.hortonworks.streamline.streams.actions.storm.topology;

import com.hortonworks.streamline.common.*;
import com.hortonworks.streamline.streams.actions.*;
import com.hortonworks.streamline.streams.catalog.*;
import com.hortonworks.streamline.streams.cluster.catalog.*;
import com.hortonworks.streamline.streams.cluster.service.*;
import com.hortonworks.streamline.streams.layout.*;
import com.hortonworks.streamline.streams.layout.beam.*;
import com.hortonworks.streamline.streams.layout.beam.ClassIterator;
import com.hortonworks.streamline.streams.layout.component.*;
import com.hortonworks.streamline.streams.layout.component.impl.testing.*;
import com.hortonworks.streamline.streams.layout.storm.*;
import com.hortonworks.streamline.streams.storm.common.*;
import org.apache.beam.sdk.*;
import org.apache.commons.lang.*;
import org.slf4j.*;

import java.io.File;
import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * Beam implementation of the TopologyActions interface
 */
public class BeamTopologyActionsImpl implements TopologyActions
{
   private static final Logger LOG = LoggerFactory.getLogger(BeamTopologyActionsImpl.class);
   public static final int DEFAULT_WAIT_TIME_SEC = 30;
   public static final int TEST_RUN_TOPOLOGY_DEFAULT_WAIT_MILLIS_FOR_SHUTDOWN = 30_000;
   public static final String ROOT_LOGGER_NAME = "ROOT";

   public static final String TOPOLOGY_EVENTLOGGER_REGISTER = "topology.event.logger.register";
   public static final String TOPOLOGY_EVENTLOGGER_CLASSNAME_STREAMLINE = "com.hortonworks.streamline.streams.runtime.storm.event.sample.StreamlineEventLogger";

   private String stormArtifactsLocation = "/tmp/storm-artifacts/";
   private String catalogRootUrl;
   private Map<String, Object> conf;


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
		 catalogRootUrl = (String) conf.get(StormTopologyLayoutConstants.YAML_KEY_CATALOG_ROOT_URL);

		/* Map<String, String> env = System.getenv();
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
		 }*/

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

   @Override
   public void deploy(TopologyLayout topology, String mavenArtifacts, TopologyActionContext ctx, String asUser)
		   throws Exception
   {
	  String xyz = null;
	  ClassIterator  cI = new ClassIterator();
	  Class[] classes = cI.getClassesInPackage("com.hortonworks.streamline.streams");
	  for (Class c : classes)
	  {
		 System.out.println("Found: " + c.getCanonicalName());
	  }
	  createYamlFile(topology, true);
	  System.out.println("");
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
	  String xyz = null;
	  System.out.println("");
   }

   @Override
   public void validate(TopologyLayout topology)
		   throws Exception
   {
	  String xyz = null;
	  System.out.println("");
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
	  String xyz = null;
	  createYamlFile(topology, true);
	  System.out.println("");
   }

   @Override
   public Status status(TopologyLayout topology, String asUser)
		   throws Exception
   {
	  StatusImpl status = new StatusImpl();
	  status.setStatus(Status.STATUS_UNKNOWN);
	  return status;
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

		 String proxyUrl = (String) conf.get(Constants.CONFIG_HTTP_PROXY_URL);
		 if (StringUtils.isNotEmpty(proxyUrl))
		 {
			args.add("--proxyUrl");
			args.add(proxyUrl);

			String proxyUsername = (String) conf.get(Constants.CONFIG_HTTP_PROXY_USERNAME);
			String proxyPassword = (String) conf.get(Constants.CONFIG_HTTP_PROXY_PASSWORD);
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
		 yamlMap.put(StormTopologyLayoutConstants.YAML_KEY_NAME, generateStormTopologyName(topology));
		 TopologyDag topologyDag = topology.getTopologyDag();
		 LOG.debug("Initial Topology config {}", topology.getConfig());
		 BeamTopologyFluxGenerator fluxGenerator = new BeamTopologyFluxGenerator(topology, conf,
				 getExtraJarsLocation(topology));
		 topologyDag.traverse(fluxGenerator);
		 Pipeline pipeline = fluxGenerator.getPipeline();
		 pipeline.run();
		 System.out.println(pipeline.toString());
		 /*for (Map.Entry<String, Map<String, Object>> entry : fluxGenerator.getYamlKeysAndComponents())
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

   private void serializePipeline(){

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
	  return StormTopologyUtil.generateStormTopologyName(topology.getId(), topology.getName());
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
	  config.put(StormTopologyLayoutConstants.YAML_KEY_CATALOG_ROOT_URL, catalogRootUrl);
	  //Hack to work around HBaseBolt expecting this map in prepare method. Fix HBaseBolt and get rid of this. We use hbase-site.xml conf from ambari
	  if (topologyConfig != null)
	  {
		 config.putAll(topologyConfig);
	  }
	  yamlMap.put(StormTopologyLayoutConstants.YAML_KEY_CONFIG, config);
   }

  /* private void addComponentToCollection(Map<String, Object> yamlMap,Component> yamlComponent, String collectionKey)
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
   }*/
}
