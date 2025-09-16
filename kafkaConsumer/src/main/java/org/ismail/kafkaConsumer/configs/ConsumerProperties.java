package org.ismail.kafkaConsumer.configs;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.ismail.kafkaConsumer.utils.MyMessage;
import java.nio.file.Paths;
import java.util.Properties;

public class ConsumerProperties {
    public final Properties fileProperties = new Properties();
    public final String path = Paths.get(System.getProperty("user.dir"), "..", "k", "ssl").toAbsolutePath().toString();
    public final String trustStorePath = Paths.get(path, "kafka.consumer.truststore.jks").toString();
    public static final String username = "dnme-consumer";
    public static final String password = "Bro1234";

    public ConsumerProperties() {
        fileProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        fileProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        fileProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
        fileProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "dnme-consumer");
        fileProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        fileProperties.setProperty("security.protocol", "SASL_SSL");
        fileProperties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        fileProperties.setProperty("ssl.truststore.location", trustStorePath);
        fileProperties.setProperty("ssl.truststore.password", "123456");
        fileProperties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username +"\" password=\"" + password + "\";");
        fileProperties.setProperty("fetch.message.max.bytes", "10485760");
        fileProperties.setProperty("json.value.type", MyMessage.class.getName());
    }
}
