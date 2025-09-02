package org.ismail.kafkaConsumer.configs;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.file.Paths;
import java.util.Properties;

public class ConsumerProperties {
    public Properties textProperties =  new Properties();
    public Properties fileProperties =  new Properties();

    public String path = Paths.get(System.getProperty("user.dir"),"..","k","ssl").toAbsolutePath().toString();
    public String trustStorePath = Paths.get(path,"kafka.consumer.truststore.jks").toString();
    public String keyStorePath = Paths.get(path,"kafka.consumer.keystore.jks").toString();

    public ConsumerProperties(){
        textProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        textProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        textProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        textProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "sasl-consumer");
        textProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        textProperties.setProperty("security.protocol", "SASL_SSL");
        textProperties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        textProperties.setProperty("ssl.truststore.location",trustStorePath);
        textProperties.setProperty("ssl.truststore.password", "123456");
        textProperties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"sasl-consumer\" password=\"Bro1234\";");


        fileProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        fileProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        fileProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        fileProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "sasl-consumer");
        fileProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        fileProperties.setProperty("security.protocol", "SASL_SSL");
        fileProperties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        fileProperties.setProperty("ssl.truststore.location",trustStorePath);
        fileProperties.setProperty("ssl.truststore.password", "123456");
        fileProperties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"sasl-consumer\" password=\"Bro1234\";");
        fileProperties.setProperty("fetch.message.max.bytes","10485760");
    }
}
