package org.ismail.kafkaProducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.ismail.kafkaProducer.configs.ProducerProperties;
import org.ismail.kafkaProducer.utils.JsonUtil;
import org.ismail.kafkaProducer.utils.MyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;

import static org.ismail.kafkaProducer.utils.Utilities.getFileType;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    private final KafkaProducer<String, byte[]> fileProducer;

    public ProducerDemo() {
        ProducerProperties producerProperties = new ProducerProperties();
        this.fileProducer = new KafkaProducer<>(producerProperties.fileProperties);
    }

    public void sendStringMessage(String topicName, String msg) throws JsonProcessingException {
        MyMessage message = new MyMessage("str", LocalDateTime.now(), msg, "sasl-producer", topicName);
        String jsonString = JsonUtil.mapper().writeValueAsString(message);
        System.out.println(message);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, jsonString.getBytes(StandardCharsets.UTF_8));
        fileProducer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                log.error("Send failed", e);
            } else {
                log.info("String sent");
            }
        });
    }


    public void sendFileMessage(String topicName, String filePath) throws IOException {
        File file = new File(filePath);
        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
        String mimeType = getFileType(filePath);

        //MyMessage bigMessage = new MyMessage(file.getName(),LocalDateTime.now(),mimeType,filePath,"sasl-producer",topicName);

        MyMessage message = new MyMessage(file.getName(), LocalDateTime.now(), mimeType, fileBytes, "sasl-producer", topicName);
        String jsonString = JsonUtil.mapper().writeValueAsString(message);
        System.out.println(message);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, jsonString.getBytes(StandardCharsets.UTF_8));
        fileProducer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                log.error("Send failed", e);
            } else {
                log.info("File sent");
            }
        });
    }

    public void close() {
        fileProducer.flush();
        fileProducer.close();
    }
}

