package com.hortonworks.streamline.streams.cluster.container;

import com.hortonworks.streamline.streams.cluster.catalog.Namespace;

import javax.security.auth.Subject;
import java.util.Map;

/**
 * @author suman.bn
 */
public interface ConfigAwareContainer<T extends NamespaceAwareContainer> {

    Map<String, Object> buildConfig(T t, Map<String, String> streamlineConf, Subject subject,
                                    Namespace namespace);

    // TODO: add generic method instantiate the implementation class.
    // ConfigAwareContainer instantiate(String streamingEngine);
}
