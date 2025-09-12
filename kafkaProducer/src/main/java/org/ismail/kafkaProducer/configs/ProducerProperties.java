package org.ismail.kafkaProducer.configs;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.file.Paths;
import java.util.Properties;

public class ProducerProperties {
    public final Properties fileProperties = new Properties();
    public final String path = Paths.get(System.getProperty("user.dir"),"..","k","ssl").toAbsolutePath().toString();
    public final String trustStorePath = Paths.get(path,"kafka.producer.truststore.jks").toString();
    public static final String username = "sasl-producer";
    public static final String password = "Bro1234";
    public ProducerProperties() {
        fileProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        fileProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        fileProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());
        fileProperties.setProperty("security.protocol", "SASL_SSL");
        fileProperties.setProperty("ssl.truststore.location",trustStorePath);
        fileProperties.setProperty("ssl.truststore.password", "123456");
        fileProperties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        fileProperties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password +"\";");
        fileProperties.setProperty("max.request.size", "10485760");
        fileProperties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        fileProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        fileProperties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000");
        fileProperties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
        fileProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "3000");
    }
}
