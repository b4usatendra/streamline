package com.hortonworks.streamline.streams.actions.beam.topology;

import com.hortonworks.streamline.streams.beam.common.BeamTopologyLayoutConstants;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;
import joptsimple.internal.Strings;

/**
 * Created by Satendra Sahu on 2/11/19
 */
public class BeamTopologyActionUtils {

    public static void validateSSLConfig(Map<String, Object> conf) {

        String securityProtocol = (String) conf.get("securityProtocol");
        if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol.endsWith("SSL")) {
            String truststoreLocation = (String) conf.get("sslTruststoreLocation");
            String truststorePassword = (String) conf.get("sslTruststorePassword");
            if (truststoreLocation == null || truststoreLocation.isEmpty()) {
                throw new IllegalArgumentException("Truststore location must be provided for SSL");
            }
            if (truststorePassword == null || truststorePassword.isEmpty()) {
                throw new IllegalArgumentException("Truststore password must be provided for SSL");
            }
        } else if (securityProtocol != null && !securityProtocol.isEmpty() && securityProtocol
            .equals("SASL_PLAINTEXT")) {

            String userName = (String) conf.get(BeamTopologyLayoutConstants.KAFKA_CLIENT_USER);
            String password = (String) conf.get(BeamTopologyLayoutConstants.KAFKA_CLIENT_PASSWORD);

            if (!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                return;
            } else {
                throw new RuntimeException("either kafka.user or password is null");
            }
        }
    }


    public static String getSaslJaasConfig(Map<String, Object> conf) {

        if (!conf.containsKey(BeamTopologyLayoutConstants.ZK_CLIENT_USER)) {
            conf.put(BeamTopologyLayoutConstants.ZK_CLIENT_USER, "zkclient");
        }

        if (!conf.containsKey(BeamTopologyLayoutConstants.ZK_CLIENT_PASSWORD)) {
            conf.put(BeamTopologyLayoutConstants.ZK_CLIENT_PASSWORD, "Ukidfds#59");
        }

        String kafkaClientUser = (String) conf.get(BeamTopologyLayoutConstants.KAFKA_CLIENT_USER);
        String kafkaClientPassword = (String) conf.get(BeamTopologyLayoutConstants.KAFKA_CLIENT_PASSWORD);
        String zkClientUser = (String) conf.get(BeamTopologyLayoutConstants.ZK_CLIENT_USER);
        String zkClientPassword = (String) conf.get(BeamTopologyLayoutConstants.ZK_CLIENT_PASSWORD);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("KafkaClient {\n")
            .append("org.apache.kafka.common.security.plain.PlainLoginModule required")
            .append("\n")
            .append("username=\"").append(kafkaClientUser).append("\"")
            .append("\n")
            .append("password=\"").append(kafkaClientPassword).append("\"")
            .append(";\n")
            .append("};\n\n");

        stringBuilder.append("Client {\n")
            .append("org.apache.zookeeper.server.auth.DigestLoginModule required")
            .append("\n")
            .append("username=\"").append(zkClientUser).append("\"")
            .append("\n")
            .append("password=\"").append(zkClientPassword).append("\"")
            .append(";\n")
            .append("};\n\n");

        return stringBuilder.toString();

    }

    public static void createJaasFile(String jaasFilePath, String jaasConfig) {

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(jaasFilePath), "utf-8"))) {
            writer.write(jaasConfig);
            //consumerJaasFile.deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException("Unable to set security protocol for kafka source: " + e.getMessage());
        }
    }
}
