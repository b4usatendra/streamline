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
package com.hortonworks.streamline.streams.beam.common;

public final class BeamTopologyLayoutConstants {

  // artifact
  public static final String BEAM_HOME_DIR = "beamHomeDir";
  public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";
  public static final String BEAM_ARTIFACTS_LOCATION_KEY = "beamArtifactsDirectory";
  public static final String BEAM_PIPELINE_JAR_LOCATION_KEY = "streamlineBeamJar";
  public static final String BEAM_PIPELINE_OPTIONS = "beamPipelineOptions";
  public static final String BEAM_JAR_FILE_TYPE_KEY = "jars";
  // yaml key constants

  // TODO: add hbase conf to topology config when processing data sinks

  //event metadata constants
  public static final String SCHEMA_FIELD = "schema";
  public static final String VERSION_FIELD = "schemaVersion";
  public static final String TOPIC_FIELD = "stream";
  public static final String TENANT_FIELD = "tenant";

  //Kafka constants
  public static final String KAFKA_CLIENT_USER = "kafka.client.user";
  public static final String KAFKA_CLIENT_PASSWORD = "kafka.client.password";

  public static final String ZK_CLIENT_USER = "zk.client.user";
  public static final String ZK_CLIENT_PASSWORD = "zk.client.password";

  public static final String JAAS_CONF_PATH = "jaas.conf.path";
}
