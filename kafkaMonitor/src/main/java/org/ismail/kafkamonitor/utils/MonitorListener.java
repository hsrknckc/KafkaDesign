package org.ismail.kafkamonitor.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.ismail.kafkamonitor.config.Props;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
                        Map<String, Boolean> readStatus = getConsumerGroupReadStatus(record.topic(),record.partition(),record.offset());
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
                        logToFile(monitoredMessage);

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

    static long time = System.currentTimeMillis();
    private void logToFile(MonitoredMessage msg) {
        JSONObject json = new JSONObject();
        json.put("topic", msg.getTopic());
        json.put("partition", msg.getPartition());
        json.put("offset", msg.getOffset());
        json.put("producer", msg.getProducer());
        json.put("textData", msg.getTextData());
        json.put("dataType", msg.getDataType());
        json.put("readStatus", msg.getConsumerGroupsReadStatus());
        json.put("prodTimeMs", msg.getProduceTimeMs());
        json.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        String logFile = "monitor_" + time + ".log";
        File file = new File(logFile);
        long size=file.length();
        if(size>1024*1024){
            time = System.currentTimeMillis();
            logFile = "monitor_" + time + ".log";
        }
        try (PrintWriter out = new PrintWriter(new FileWriter(logFile, true))) {
            out.println(json);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public Map<String, Boolean> getConsumerGroupReadStatus(String topic ,int partition, long offset) throws ExecutionException, InterruptedException {
        Map<String, Boolean> status = new HashMap<>();
        ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();

        TopicPartition tp = new TopicPartition(topic, partition);

        for (var groupListing : groupsResult.all().get()) {
            String groupId = groupListing.groupId();
            if (groupId.equals("monitor-consumer-group")) continue;

            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
            var offsets = offsetsResult.partitionsToOffsetAndMetadata().get();

            boolean hasRead= false;
            if(offsets.containsKey(tp)){
                long committed = offsets.get(tp).offset();
                hasRead = committed > offset;
            }
            status.put(groupId, hasRead);
        }
        return status;
    }
}
