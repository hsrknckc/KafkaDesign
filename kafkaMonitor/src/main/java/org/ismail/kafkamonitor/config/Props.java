package org.ismail.kafkamonitor.config;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.ismail.kafkamonitor.utils.MyMessage;

import java.nio.file.Paths;
import java.util.Properties;

public class Props {
    public final Properties properties = new Properties();
    public final String path = Paths.get(System.getProperty("user.dir"),"..","k","ssl").toAbsolutePath().toString();
    public final String trustStorePath = Paths.get(path,"kafka.broker.truststore.jks").toString();

    public final Properties monitorProperties = new Properties();
    public final String trustStorePath2 = Paths.get(path,"kafka.consumer.truststore.jks").toString();


    public Props() {
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("ssl.truststore.location",trustStorePath);
        properties.setProperty("ssl.truststore.password", "123456");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"broker-admin\" password=\"Bro123\";");

        monitorProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        monitorProperties.setProperty("security.protocol", "SASL_SSL");
        monitorProperties.setProperty("ssl.truststore.location",trustStorePath2);
        monitorProperties.setProperty("ssl.truststore.password", "123456");
        monitorProperties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        monitorProperties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"broker-admin\" password=\"Bro123\";");
        monitorProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        monitorProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
        monitorProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"monitor-consumer-group");
        monitorProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        monitorProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        monitorProperties.setProperty("json.value.type", MyMessage.class.getName());




    }
}
