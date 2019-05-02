/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
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


    public static final String DEFAULT_BEAM_ARTIFACTS_LOCATION = "/tmp/beam-artifacts";
    public static final String FLINK_MASTER_KEY = "master.endpoint";
    public static final String TEMP_TOPLOGY_PATH = "/tmp/topology/%s/%s/";
    public static final String TOPOLOGY_SERIALIZED_OBJECT_PATH = "/serializedObject";
    public static final String FLINK_HOME_DIR = "flinkHomeDir";



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

    //Flink config properties
    public static final String JSON_KEY_YARN_CONTAINER = "yarn.container";
    public static final String JSON_KEY_YARN_SLOTS = "yarn.slots";
    public static final String JSON_KEY_YARN_JOB_MANAGER_MEMORY = "yarn.job.manager.memory";
    public static final String JSON_KEY_YARN_TASK_MANAGER_MEMORY = "yarn.task.manager.memory";

    //flink CLI args
    public static final String SHUTDOWN_HOOK = "--sae";
    public static final String YARN_CLUSTER = "yarn-cluster";
    public static final String YARN_CONTAINER = "--yarncontainer";
    public static final String YARN_SLOTS = "--yarnslots";
    public static final String YARN_JOB_MANAGER_MEMORY = "--yarnjobManagerMemory";
    public static final String YARN_TASK_MANAGER_MEMORY = "--yarntaskManagerMemory";
    public static final String YARN_NAME = "--yarnname";
    public static final String FLINK_MASTER = "--flinkMaster";

    //BEAM Constants
    public static final String RUNNER = "--runner";
    public static final String FILES_TO_STAGE = "--filesToStage";



}
