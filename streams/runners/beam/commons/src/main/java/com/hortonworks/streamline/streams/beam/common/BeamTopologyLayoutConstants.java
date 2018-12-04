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
package com.hortonworks.streamline.streams.beam.common;

public final class BeamTopologyLayoutConstants {

    // artifact
    public static final String BEAM_HOME_DIR = "beamHomeDir";
    public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";
    public static final String STREAMLINE_COMPONENT_CONF_KEY = "streamlineComponent";
    public static final String BEAM_ARTIFACTS_LOCATION_KEY = "beamArtifactsDirectory";
    public static final String BEAM_PIPELINE_JAR_LOCATION_KEY = "streamlineBeamJar";

    // yaml key constants
    public static final String YAML_KEY_NAME = "name";
    public static final String YAML_KEY_VALUE = "value";
    public static final String YAML_KEY_CATALOG_ROOT_URL = "catalog.root.url";
    public static final String YAML_KEY_LOCAL_PARSER_JAR_PATH = "local.parser.jar.path";
    // TODO: add hbase conf to topology config when processing data sinks
    public static final String YAML_KEY_BEAM_SOURCE = "beam.source";
    public static final String YAML_KEY_BEAM_SINK = "beam.sink";
    public static final String YAML_KEY_BEAM_PROCESSOR = "beam.processor";
    public static final String YAML_KEY_CONFIG = "config";
    public static final String YAML_KEY_COMPONENTS = "components";
    public static final String YAML_KEY_STREAMS = "streams";
    public static final String YAML_KEY_ID = "id";
    public static final String YAML_KEY_CLASS_NAME = "className";
    public static final String YAML_KEY_PROPERTIES = "properties";
    public static final String YAML_KEY_CONSTRUCTOR_ARGS = "constructorArgs";
    public static final String YAML_KEY_REF = "ref";
    public static final String YAML_KEY_REF_LIST = "reflist";
    public static final String YAML_KEY_ARGS = "args";
    public static final String YAML_KEY_CONFIG_METHODS = "configMethods";
    public static final String YAML_KEY_FROM = "from";
    public static final String YAML_KEY_TO = "to";
    public static final String YAML_KEY_GROUPING = "grouping";
    public static final String YAML_KEY_ALL_GROUPING = "ALL";
    public static final String YAML_KEY_CUSTOM_GROUPING = "CUSTOM";
    public static final String YAML_KEY_DIRECT_GROUPING = "DIRECT";
    public static final String YAML_KEY_SHUFFLE_GROUPING = "SHUFFLE";
    public static final String YAML_KEY_LOCAL_OR_SHUFFLE_GROUPING = "LOCAL_OR_SHUFFLE";
    public static final String YAML_KEY_FIELDS_GROUPING = "FIELDS";
    public static final String YAML_KEY_GLOBAL_GROUPING = "GLOBAL";
    public static final String YAML_KEY_NONE_GROUPING = "NONE";
    public static final String YAML_KEY_TYPE = "type";
    public static final String YAML_PARSED_TUPLES_STREAM = "parsed_tuples_stream";
    public static final String YAML_FAILED_TO_PARSE_TUPLES_STREAM = "failed_to_parse_tuples_stream";
    public static final String YAML_KEY_STREAM_ID = "streamId";
    public final static String YAML_KEY_PARALLELISM = "parallelism";
    public final static String YAML_KEY_CUSTOM_GROUPING_CLASS = "customClass";
    public final static String YAML_KEY_CUSTOM_GROUPING_CLASSNAME = "";
    private BeamTopologyLayoutConstants() {
    }
}
