package com.hortonworks.streamline.streams.runtime.beam;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.FabricEventImpl;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.common.event.sedes.kafka.FabricEventAvroSerializer;
import com.hortonworks.streamline.streams.common.event.sedes.kafka.FabricEventJsonDeserializer;
import com.hortonworks.streamline.streams.common.event.sedes.kafka.FabricEventJsonSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Created by Satendra Sahu on 12/6/18
 */
public class KafkaProducerTest {

    Producer<String, StreamlineEvent> producer;
    Random random;
    private final ObjectMapper mapper = new ObjectMapper();
    private String fabricEvent = "{\n"
        + "\t\"id\": \"51fd2bc8-3000-4c5a-81cb-0d3f200859a5\",\n"
        + "\t\"metadata\": {\n"
        + "\t\t\"timestamp\": 1550038067418,\n"
        + "\t\t\"schema\": \"filter_booking_rakam\",\n"
        + "\t\t\"schemaVersion\": 1,\n"
        + "\t\t\"type\": \"EVENT\",\n"
        + "\t\t\"routingKey\": {\n"
        + "\t\t\t\"type\": \"simple\",\n"
        + "\t\t\t\"value\": \"1550038067418\"\n"
        + "\t\t},\n"
        + "\t\t\"lookupKey\": {\n"
        + "\t\t\t\"type\": null,\n"
        + "\t\t\t\"value\": \"00f0e972-342d-44f4-9e19-96261347ad43\"\n"
        + "\t\t},\n"
        + "\t\t\"tenant\": \"sharedriver_instrumentation\",\n"
        + "\t\t\"stream\": \"sharedriver_instrumentation_rakam\",\n"
        + "\t\t\"sender\": \"testSender\"\n"
        + "\t},\n"
        + "\t\"data\": {\n"
        + "\t\t\"machine\": \"testMachine\",\n"
        + "\t\t\"machine_COUNT\": 1124235123513,\n"
        + "\t\t\"building_MAX\": \"MAX\"\n"
        + "\t}\n"
        + "}";

    String fabricEvent1 = "{\n"
        + "\t\"id\": \"9dcd880f-d6d2-4a26-91bb-f0b2634c87ac\",\n"
        + "\t\"metadata\": {\n"
        + "\t\t\"timestamp\": 1556094996146,\n"
        + "\t\t\"schema\": \"ams_status_change\",\n"
        + "\t\t\"schemaVersion\": 1,\n"
        + "\t\t\"type\": \"EVENT\",\n"
        + "\t\t\"lookupKey\": {\n"
        + "\t\t\t\"type\": \"simple\",\n"
        + "\t\t\t\"value\": \"9dcd880f-d6d2-4a26-91bb-f0b2634c87ac\"\n"
        + "\t\t},\n"
        + "\t\t\"tenant\": \"ams\",\n"
        + "\t\t\"stream\": \"ams_status_change\",\n"
        + "\t\t\"sender\": \"ams\"\n"
        + "\t},\n"
        + "\t\"data\": {\n"
        + "\t\t\"user_id\": \"Pharos\",\n"
        + "\t\t\"dialing_code\": 900,\n"
        + "\t\t\"email_id\": \"abc@xyz.com\"\n"
        + "\t}\n"
        + "}";

    private Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, FabricEventJsonSerializer.class);
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "C1MNV1CUDTY3.local:9092");
        System.setProperty("java.security.auth.login.config",
            "/Users/satendra.sahu/code/github/streamline/conf/jaas.conf");

        properties.put("schema.registry.url", "http://localhost:8877/api/v1");
        return properties;
    }

    public void init() {
        producer = new KafkaProducer<String, StreamlineEvent>(getProducerProperties());
        random = new Random();
    }

    private ProducerRecord<String, StreamlineEvent> getFabricEvent() {

        Map<String, Object> metadata = new HashMap<>();

        metadata.put("timestamp", System.currentTimeMillis());
        metadata.put("schema", "fabric_doc_structure");
        metadata.put("schemaVersion", 1);
        metadata.put("type", "EVENT");
        metadata.put("tenant", "steem_producer");
        metadata.put("sender", "test_sender");
        metadata.put("stream", "beam_test_input");
        Map<String, Object> routingKey = new HashMap<>();

        routingKey.put("type", null);
        routingKey.put("value", "463SG_iYQ5bATUlfgz4vxFCLE36MP0k9ZkEONIpgOuU");

        metadata.put("routingKey", routingKey);


        Map<String, Object> lookupKey = new HashMap<>();
        lookupKey.put("type",  null);
        lookupKey.put("value", "FCLE36MP0k9ZkEONIpgOuU463SG_iYQ5bATUlfgz4vx");

        metadata.put("lookupKey", null);

        FabricEventImpl event = FabricEventImpl.builder().dataSourceId("1")
            .sourceStream("testProducer")
            .header(metadata)
            .put("email_id", "test@xyz.com")
            .put("dialing_code", random.nextInt())
            .put("user_id", "test_user")
            .build();

        return new ProducerRecord<>("beam_test_input", "testMsgKey", event);
    }

    private ProducerRecord<String, String> getFabricRecord() {
        return new ProducerRecord<>("beam_test_input", "testMsgKey", fabricEvent1);
    }

    public void send() {
        producer.send(
            getFabricEvent());
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaProducerTest kafkaProducerTest = new KafkaProducerTest();
        kafkaProducerTest.init();
        while (true) {
            kafkaProducerTest.send();
            Thread.sleep(1000);
        }
    }
}
