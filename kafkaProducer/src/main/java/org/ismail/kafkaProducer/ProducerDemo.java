package org.ismail.kafkaProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.ismail.kafkaProducer.configs.ProducerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    private final KafkaProducer<String, String> textProducer;
    private final KafkaProducer<String, byte[]> fileProducer;

    public ProducerDemo() {
        ProducerProperties producerProperties = new ProducerProperties();

        this.textProducer = new KafkaProducer<>(producerProperties.textProperties);
        this.fileProducer = new KafkaProducer<>(producerProperties.fileProperties);
    }

    public void sendStringMessage(String topicName, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName,"text", message);
        textProducer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                log.error("Send failed", e);
            } else {
                log.info("String sent");
            }
        });
    }

    public String getFileType(String filePath) throws IOException {
        Path path= Paths.get(filePath);
        String mimeType = Files.probeContentType(path);

        if(mimeType==null) return "bilinmeyen dosya tipi";

        if(mimeType.startsWith("text") ) return "text";
        else if(mimeType.startsWith("image") ) return "image";
        else if(mimeType.startsWith("pdf") ) return "pdf";
        else return "desteklenmeyen dosya tipi";
    }

    public void sendFileMessage(String topicName, String filePath) throws IOException {
        byte[] fileBytes= Files.readAllBytes(Paths.get(filePath));
        String mimeType = getFileType(filePath);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName,mimeType,fileBytes);
        fileProducer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                log.error("Send failed", e);
            }else{
                log.info("File sent");
            }
        });
    }

    public void close() {
        textProducer.flush();
        textProducer.close();
    }
}

