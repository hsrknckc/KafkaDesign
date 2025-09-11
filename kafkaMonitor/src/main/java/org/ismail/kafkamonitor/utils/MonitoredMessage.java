package org.ismail.kafkamonitor.utils;

import java.time.LocalDateTime;
import java.util.Map;

public class MonitoredMessage {
    private final String topic;
    private final long offset;
    private final LocalDateTime timestamp;
    private Map<String, Boolean> consumerGroupsReadStatus;
    private final String producer;
    private String textData;
    private final long produceTimeMs;

    private String fileId;
    private String dataType;
    private byte[] data;
    private String dataPath;
    private String name;
    private int chunkNumber;
    private int totalChunk;
    public static final int chunkSize= 1024000;

    public MonitoredMessage(String topic, long offset, String producer, LocalDateTime timestamp, String textData, Map<String, Boolean> consumerGroupsReadStatus, long produceTimeMs) {
        this.topic = topic;
        this.offset = offset;
        this.timestamp = timestamp;
        this.consumerGroupsReadStatus = consumerGroupsReadStatus;
        this.producer = producer;
        this.produceTimeMs = produceTimeMs;
//        if(textData != null) this.textData = textData;
//        else this.textData = "!!!gösterilemeyen dosya!!!";
        this.textData = textData;
        this.dataType = "string";
        this.name = "str";
    }

    public MonitoredMessage(String name,String topic, long offset, String producer, LocalDateTime timestamp, String dataType, byte[] data,String fileId ,Map<String, Boolean> consumerGroupsReadStatus, long produceTimeMs,int chunkNumber,int totalChunk) {
        this.topic = topic;
        this.offset = offset;
        this.timestamp = timestamp;
        this.consumerGroupsReadStatus = consumerGroupsReadStatus;
        this.producer = producer;
        this.produceTimeMs = produceTimeMs;
//        if(textData != null) this.textData = textData;
//        else this.textData = "!!!gösterilemeyen dosya!!!";
        this.data = data;
        this.dataType = dataType;
        this.name = name;
        this.fileId = fileId;
        this.chunkNumber = chunkNumber;
        this.totalChunk = totalChunk;

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
    public String getTextData() { return textData; }
    public long getProduceTimeMs() { return produceTimeMs; }
    public int getTotalChunk() { return totalChunk; }
    public void setTotalChunk(int totalChunk) { this.totalChunk = totalChunk; }
    public int getChunkNumber() { return chunkNumber; }
    public void setChunkNumber(int chunkNumber) { this.chunkNumber = chunkNumber; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDataPath() { return dataPath; }
    public void setDataPath(String dataPath) { this.dataPath = dataPath; }
    public byte[] getData() { return data; }
    public void setData(byte[] data) { this.data = data; }
    public String getDataType() { return dataType; }
    public void setDataType(String dataType) { this.dataType = dataType; }
    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }
}

