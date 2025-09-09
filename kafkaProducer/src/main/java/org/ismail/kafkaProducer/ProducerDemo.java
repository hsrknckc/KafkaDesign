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
import java.util.*;

import static org.ismail.kafkaProducer.utils.Utilities.getFileType;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    private final KafkaProducer<String, byte[]> fileProducer;
    public final List<Long> produceSpeed;

    public ProducerDemo() {
        ProducerProperties producerProperties = new ProducerProperties();
        this.fileProducer = new KafkaProducer<>(producerProperties.fileProperties);
        produceSpeed = new ArrayList<>();
    }

    public void sendStringMessage(String topicName, String msg, AckCallback callback) throws JsonProcessingException {
        MyMessage message = new MyMessage("str", LocalDateTime.now(), msg, "sasl-producer", topicName);
        String jsonString = JsonUtil.mapper().writeValueAsString(message);
        System.out.println(message);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, jsonString.getBytes(StandardCharsets.UTF_8));
        fileProducer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                log.error("Send failed", e);
                if(callback != null) callback.onFail(e.getMessage());
            } else {
                log.info("String sent, offset: {}", recordMetadata.offset());
                if(callback != null) callback.onSuccess(recordMetadata.offset());
            }
        });
    }


    public void sendFileMessage(String topicName, String filePath,AckCallback callback) throws IOException {
        File file = new File(filePath);
        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
        System.out.println(fileBytes.length);
        String mimeType = getFileType(filePath);
        String fileId = UUID.randomUUID().toString();

        int totalChunk = (int) Math.ceil((double)fileBytes.length / MyMessage.chunkSize);
        System.out.println(totalChunk);
        long startTime= System.nanoTime();
        for(int i = 0; i < totalChunk; i++) {
            int start = i * MyMessage.chunkSize;
            int end = Math.min(start + MyMessage.chunkSize, fileBytes.length);
            byte[] chunkedFileBytes = Arrays.copyOfRange(fileBytes, start, end);
            System.out.println(chunkedFileBytes.length);

            MyMessage message = new MyMessage(file.getName(), LocalDateTime.now(), mimeType, chunkedFileBytes, "sasl-producer", topicName,fileId,i,totalChunk);

            message.setFileId(fileId);
            System.out.println("file id: " + message.getFileId());
            message.setChunkNumber(i);
            System.out.println(message.getChunkNumber());
            message.setTotalChunk(totalChunk);
            System.out.println(message.getTotalChunk());

            String jsonString = JsonUtil.mapper().writeValueAsString(message);
            System.out.println(message);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, jsonString.getBytes(StandardCharsets.UTF_8));

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
        long endTime= System.nanoTime();
        produceSpeed.add((endTime - startTime)/1000000);
        System.out.println("geçen süre:" + ((endTime - startTime)/1000000) + "ms");
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

