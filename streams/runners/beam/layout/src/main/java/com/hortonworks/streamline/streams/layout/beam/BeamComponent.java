package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.common.exception.ComponentConfigException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Map;

/**
 * Created by Satendra Sahu on 10/26/18
 */
public interface BeamComponent {
    /*
     Initialize the implementation with catalog root url
      */
    void withCatalogRootUrl(String catalogRootUrl);

    /*
    Method to initialize the implementation with a configuration
     */
    void withConfig(Map<String, Object> config, Pipeline pipeline);

   /*
   Get yaml maps of all the components referenced by this component
   Expected to return equivalent of something like below.
   - id: "zkHosts"
   className: "org.apache.storm.kafka.ZkHosts"
   constructorArgs:
	 - ${kafka.spout.zkUrl}

   - id: "spoutConfig"
   className: "org.apache.storm.kafka.SpoutConfig"
   constructorArgs:
	 - ref: "zkHosts"
	*/

    /*
    Get yaml map for this component. Note that the id field will be
    overwritten and hence is optional.
    Expected to return equivalent of something like below
    - id: "KafkaSpout"
    className: "org.apache.storm.kafka.KafkaSpout"
    constructorArgs:
      - ref: "spoutConfig"
     */
    PCollection<KV<String, String>> getOutputCollection();

    public void generateComponent(PCollection pCollection);

    void unionInputCollection(PCollection<KV<String, String>> inputCollection);

    /*
    validate the configuration for this component.
    throw ComponentConfigException if configuration is not correct
     */
    void validateConfig()
            throws ComponentConfigException;
}
