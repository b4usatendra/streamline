package com.hortonworks.streamline.streams.layout.beam;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * Created by satendra.sahu on 2019-04-29
 */
public class BeamLayoutUtils {


    public static String getSaslJaasConfig(String kafkaClientUser, String kafkaClientPassword,
        String zkClientUser, String zkClientPassword) {

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
            new FileOutputStream(jaasFilePath+ "/jaas.conf"), "utf-8"))) {
            writer.write(jaasConfig);
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to set security protocol for kafka source: " + e.getMessage());
        }
    }

}
