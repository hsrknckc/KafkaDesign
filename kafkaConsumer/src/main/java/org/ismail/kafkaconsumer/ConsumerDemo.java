package org.ismail.kafkaconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ismail.kafkaconsumer.configs.ConsumerProperties;
import org.ismail.kafkaconsumer.utils.JsonUtil;
import org.ismail.kafkaconsumer.utils.MyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.function.Consumer;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    private final KafkaConsumer<String, String> textConsumer;
    private final KafkaConsumer<String, byte[]> fileConsumer;

    public ConsumerDemo() {
        ConsumerProperties consumerProperties = new ConsumerProperties();
        this.textConsumer = new KafkaConsumer<>(consumerProperties.textProperties);
        this.fileConsumer = new KafkaConsumer<>(consumerProperties.fileProperties);
    }


//    public void readStringMessages(String topicName, Consumer<String> onMessageReceived) {
//        textConsumer.subscribe(Collections.singletonList(topicName));
//        while (true) {
//            ConsumerRecords<String, String> records = textConsumer.poll(Duration.ofMillis(500));
//            for (ConsumerRecord<String, String> record : records) {
//                log.info(record.value());
//                onMessageReceived.accept(record.value());
//            }
//        }
//    }
    public void readFileMessages(String topicName, Consumer<MyMessage> onFileReceived) throws JsonProcessingException {
        fileConsumer.subscribe(Collections.singletonList(topicName));
        while (true) {
            ConsumerRecords<String, byte[]> records = fileConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    String jsonStr = new String(record.value(), StandardCharsets.UTF_8);
                    MyMessage msg = JsonUtil.mapper().readValue(jsonStr, MyMessage.class);
                    log.info("File received: {}", msg.toString());
                    onFileReceived.accept(msg);
                }catch (JsonProcessingException e){
                    log.error("JSON hatası + ", e);
                }
            }
        }
    }

    public void close(){
        textConsumer.close();
        fileConsumer.close();
    }
}
