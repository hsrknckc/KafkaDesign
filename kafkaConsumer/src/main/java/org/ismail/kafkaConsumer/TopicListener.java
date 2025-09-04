package org.ismail.kafkaConsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ismail.kafkaConsumer.configs.ConsumerProperties;
import org.ismail.kafkaConsumer.utils.JsonUtil;
import org.ismail.kafkaConsumer.utils.MyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class TopicListener implements Runnable {
    private final KafkaConsumer<String, byte[]> consumer;
    private final String topicName;
    private final Consumer<MyMessage> callback;
    private volatile boolean running = true;
    private final Logger logger;

    public TopicListener(String topicName, Consumer<MyMessage> callback) {
        ConsumerProperties consumerProperties = new ConsumerProperties();
        logger = LoggerFactory.getLogger(TopicListener.class);
        this.consumer = new KafkaConsumer<>(consumerProperties.fileProperties);
        this.topicName = topicName;
        this.callback = callback;
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topicName));

        Map<String, List<MyMessage>> fileChunks = new ConcurrentHashMap<>();
        Map<String,Long> lastUpdate = new ConcurrentHashMap<>();

        while (running) {
            ConsumerRecords<String, byte[]> records = consumer.poll(java.time.Duration.ofMillis(500));
            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    String jsonStr = new String(record.value(), StandardCharsets.UTF_8);
                    MyMessage msg = JsonUtil.mapper().readValue(jsonStr, MyMessage.class);
                    System.out.println("File received: " + msg.toString());

                    System.out.println("file id: " + msg.getFileId());
                    String fileId= msg.getFileId();
                    fileChunks.putIfAbsent(fileId,new ArrayList<>());
                    fileChunks.get(fileId).add(msg);

                    if(fileChunks.get(fileId).size()==msg.getTotalChunk()){
                        List<MyMessage> sortedChunks = fileChunks.get(fileId).stream()
                                .sorted(Comparator.comparingInt(MyMessage::getChunkNumber))
                                .toList();

                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        for(MyMessage chunk : sortedChunks){
                            outputStream.write(chunk.getData());
                        }
                        byte[] fullFile = outputStream.toByteArray();

                        String folder = "downloads";
                        Files.createDirectories(Paths.get(folder));
                        String filePath = folder + File.separator + msg.getName();
                        Files.write(Paths.get(filePath), fullFile);

                        System.out.println("Dosya birleştirildi" );

                        fileChunks.remove(fileId);

                    }

                    callback.accept(msg);
                } catch (JsonProcessingException e) {
                    logger.error(e.getMessage());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        consumer.close();
    }

    public void stop() {
        running = false;
    }
}
