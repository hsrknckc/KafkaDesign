package org.ismail.kafkamonitor.utils;

import java.time.LocalDateTime;
import java.util.Map;

public class MonitoredMessage {
    private final String topic;
    private final long offset;
    private final LocalDateTime timestamp;
    private Map<String, Boolean> consumerGroupsReadStatus;
    private final String producer;

    public MonitoredMessage(String topic, long offset, String producer, LocalDateTime timestamp, String data, Map<String, Boolean> consumerGroupsReadStatus) {
        this.topic = topic;
        this.offset = offset;
        this.timestamp = timestamp;
        this.consumerGroupsReadStatus = consumerGroupsReadStatus;
        this.producer = producer;
    }


    public void updateReadStatus(MonitorListener listener){
        try{
            this.consumerGroupsReadStatus = listener.getConsumerGroupReadStatus(getTopic(),getOffset());
        }catch (Exception e){
            System.err.println("Error while updating read status : " + e.getMessage());
        }
    }

    // Getter ve Setter’lar
    public String getTopic() { return topic; }
    public long getOffset() { return offset; }
    public LocalDateTime getTimestamp() { return timestamp; }
    public Map<String, Boolean> getConsumerGroupsReadStatus() { return consumerGroupsReadStatus; }
    public String getProducer() { return producer; }
}

