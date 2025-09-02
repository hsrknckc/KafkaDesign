package org.ismail.kafkaConsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ismail.kafkaConsumer.configs.ConsumerProperties;
import org.ismail.kafkaConsumer.utils.JsonUtil;
import org.ismail.kafkaConsumer.utils.MyMessage;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.function.Consumer;

public class TopicListener implements Runnable {
    private final KafkaConsumer<String, byte[]> consumer;
    private final String topicName;
    private final Consumer<MyMessage> callback;
    private volatile boolean running = true;

    public TopicListener(String topicName, Consumer<MyMessage> callback) {
        ConsumerProperties consumerProperties = new ConsumerProperties();
        this.consumer = new KafkaConsumer<>(consumerProperties.fileProperties);
        this.topicName = topicName;
        this.callback = callback;
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topicName));
        while (running) {
            ConsumerRecords<String, byte[]> records = consumer.poll(java.time.Duration.ofMillis(500));
            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    String jsonStr = new String(record.value(), StandardCharsets.UTF_8);
                    MyMessage msg = JsonUtil.mapper().readValue(jsonStr, MyMessage.class);
                    System.out.println("File received: " + msg.toString());
                    callback.accept(msg);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
        consumer.close();
    }

    public void stop() {
        running = false;
    }
}
