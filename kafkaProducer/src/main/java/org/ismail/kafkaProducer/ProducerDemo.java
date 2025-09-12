package org.ismail.kafkaProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.ismail.kafkaProducer.configs.ProducerProperties;
import org.ismail.kafkaProducer.utils.MyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;

import static org.ismail.kafkaProducer.utils.Utilities.getFileType;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    private final KafkaProducer<String, MyMessage> fileProducer;

    public ProducerDemo() { this.fileProducer = new KafkaProducer<>(new ProducerProperties().fileProperties); }

    public void sendStringMessage(String topicName, String msg, AckCallback callback) {
        MyMessage message = new MyMessage("str", LocalDateTime.now(), msg, ProducerProperties.username, topicName);
        long startTime = System.currentTimeMillis();
        message.setProduceTime(startTime);
        ProducerRecord<String, MyMessage> record = new ProducerRecord<>(topicName, message);
        fileProducer.send(record, (recordMetadata,e) -> {
            if (e != null) {
                log.error("Send failed", e);
                if(callback != null) callback.onFail(e.getMessage());
            } else {
                log.info("String sent, offset: {}", recordMetadata.offset());
                if(callback != null) callback.onSuccess(recordMetadata.offset());
            }
        });
    }

    public void sendFileMessage(String topicName, String filePath, AckCallback callback) throws IOException {
        File file = new File(filePath);
        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
        String mimeType = getFileType(filePath);
        String fileId = UUID.randomUUID().toString();

        int totalChunk = (int) Math.ceil((double)fileBytes.length / MyMessage.chunkSize);
        for(int i = 0; i < totalChunk; i++) {
            int start = i * MyMessage.chunkSize;
            int end = Math.min(start + MyMessage.chunkSize, fileBytes.length);
            byte[] chunkedFileBytes = Arrays.copyOfRange(fileBytes, start, end);

            MyMessage message = new MyMessage(file.getName(), LocalDateTime.now(), mimeType, chunkedFileBytes, ProducerProperties.username, topicName,fileId,i,totalChunk);
            long startTime = System.currentTimeMillis();
            message.setProduceTime(startTime);

            ProducerRecord<String, MyMessage> record = new ProducerRecord<>(topicName, message);
            int chunkNumber = i;
            fileProducer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    log.error("Chunk {} send failed ", chunkNumber, e);
                    if(callback != null) callback.onFail("Chunk " + chunkNumber + ": " + e.getMessage());
                } else {
                    log.info("Chunk {} sent, offset: {}", chunkNumber, recordMetadata.offset());
                    if(callback != null) callback.onSuccess(recordMetadata.offset());
                }
            });
        }
    }

    public void close() {
        fileProducer.flush();
        fileProducer.close();
    }

    public interface AckCallback{
        void onSuccess(long offset);
        void onFail(String error);
    }
}

