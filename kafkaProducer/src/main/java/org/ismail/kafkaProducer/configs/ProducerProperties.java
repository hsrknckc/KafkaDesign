package org.ismail.kafkaProducer.configs;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.file.Paths;
import java.util.Properties;

public class ProducerProperties {
    public Properties textProperties = new Properties();
    public Properties fileProperties = new Properties();
    public String path = Paths.get(System.getProperty("user.dir"),"..","k","ssl").toAbsolutePath().toString();
    public String trustStorePath = Paths.get(path,"kafka.producer.truststore.jks").toString();
    public String keyStorePath = Paths.get(path,"kafka.producer.keystore.jks").toString();
    public ProducerProperties() {
        textProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        textProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        textProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        textProperties.setProperty("security.protocol", "SASL_SSL");
        textProperties.setProperty("ssl.truststore.location", trustStorePath);
        textProperties.setProperty("ssl.truststore.password", "123456");
        textProperties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        textProperties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"sasl-producer\" password=\"Bro1234\";");

        fileProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        fileProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        fileProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        fileProperties.setProperty("security.protocol", "SASL_SSL");
        fileProperties.setProperty("ssl.truststore.location",trustStorePath);
        fileProperties.setProperty("ssl.truststore.password", "123456");
        fileProperties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        fileProperties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"sasl-producer\" password=\"Bro1234\";");
        fileProperties.setProperty("max.request.size", "10485760");
    }
}
