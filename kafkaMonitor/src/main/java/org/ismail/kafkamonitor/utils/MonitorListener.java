package org.ismail.kafkamonitor.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.ismail.kafkamonitor.config.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class MonitorListener implements Runnable {
    private volatile boolean running = true;
    private final KafkaConsumer<String, MyMessage> consumer;
    private final AdminClient adminClient;
    private final String topic;
    private final Logger logger;
    private final Consumer<MonitoredMessage> callback;

    public MonitorListener(String topic, Consumer<MonitoredMessage> callback) {
        this.adminClient = AdminClient.create(new Props().properties);
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(new Props().monitorProperties);
        logger = LoggerFactory.getLogger(MonitorListener.class);
        this.callback = callback;
    }

    @Override
    public void run() {
        try{
            consumer.subscribe(Collections.singletonList(topic));
            while (running) {
                ConsumerRecords<String, MyMessage> records = consumer.poll(java.time.Duration.ofMillis(5000));
                for (ConsumerRecord<String, MyMessage> record : records) {
                    try {
                        MyMessage msg = record.value();
                        Map<String, Boolean> readStatus = getConsumerGroupReadStatus(topic, record.offset());
                        MonitoredMessage monitoredMessage;
                        if(msg.getName().equals("str")) {
                            monitoredMessage = new MonitoredMessage(
                                    topic,
                                    record.partition(),
                                    record.offset(),
                                    msg.getProducer(),
                                    msg.getTime(),
                                    msg.getTextData(),
                                    readStatus,
                                    (record.timestamp() - msg.getProduceTime())
                            );
                        }else{
                            monitoredMessage = new MonitoredMessage(
                                    msg.getName(),
                                    topic,
                                    record.partition(),
                                    record.offset(),
                                    msg.getProducer(),
                                    msg.getTime(),
                                    msg.getDataType(),
                                    msg.getData(),
                                    msg.getFileId(),
                                    readStatus,
                                    (record.timestamp() - msg.getProduceTime()),
                                    msg.getChunkNumber(),
                                    msg.getTotalChunk()
                            );
                        }
                        callback.accept(monitoredMessage);

                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                }
        }}catch (WakeupException e){
            if(running) throw e;
        }finally {
            consumer.close();
        }
    }

    public void stop(){
        running = false;
        consumer.wakeup();
    }

    public Map<String, Boolean> getConsumerGroupReadStatus(String topic, long offset) throws ExecutionException, InterruptedException {
        Map<String, Boolean> status = new HashMap<>();
        ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();

        for (var groupListing : groupsResult.all().get()) {
            String groupId = groupListing.groupId();
            if (groupId.equals("monitor-consumer-group")) continue;
            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
            var offsets = offsetsResult.partitionsToOffsetAndMetadata().get();

            var tp = new org.apache.kafka.common.TopicPartition(topic, 0); // partition 0 için örnek
            if (offsets.containsKey(tp)) {
                long committed = offsets.get(tp).offset();
                status.put(groupId, committed > offset);
            } else {
                status.put(groupId, false);
            }
        }
        return status;
    }
}
