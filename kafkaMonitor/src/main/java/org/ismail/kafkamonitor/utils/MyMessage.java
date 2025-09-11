package org.ismail.kafkamonitor.utils;

import java.time.LocalDateTime;

public class MyMessage {
    private String fileId;
    private LocalDateTime time;
    private String dataType;
    private byte[] data;
    private String textData;
    private String producer;
    private String topic;
    private String dataPath;
    private String name;
    private int chunkNumber;
    private int totalChunk;
    private long produceTime;
    public static final int chunkSize = 1024000;


    public MyMessage() {}

    public MyMessage(String name, LocalDateTime time, String dataType, byte[] data, String producer, String topic, String fileId, int chunkNumber, int totalChunk) {
        this.name = name;
        this.time = time;
        this.dataType = dataType;
        this.data = data;
        this.producer = producer;
        this.topic = topic;
        this.fileId = fileId;
        this.chunkNumber = chunkNumber;
        this.totalChunk = totalChunk;
    }

    public MyMessage(String name, LocalDateTime time, String dataType, String dataPath, String producer, String topic) {
        this.name = name;
        this.time = time;
        this.dataType = dataType;
        this.dataPath = dataPath;
        this.producer = producer;
        this.topic = topic;
    }

    public MyMessage(String name, LocalDateTime time, String textData, String producer, String topic) {
        this.name = name;
        this.time = time;
        this.dataType = "string";
        this.textData = textData;
        this.producer = producer;
        this.topic = topic;
    }

    public LocalDateTime getTime() {
        return time;
    }
    public String getDataType() { return dataType; }
    public byte[] getData() { return data; }
    public String getTextData() { return textData; }
    public String getDataPath() { return dataPath; }
    public String getProducer() { return producer; }
    public String getTopic() { return topic; }
    public String getName() { return name; }
    public int getChunkNumber() { return chunkNumber; }
    public int getTotalChunk() { return totalChunk; }
    public String getFileId() { return fileId; }
    public long getProduceTime() { return produceTime; }
    public void setProduceTime(long produceTime) { this.produceTime = produceTime; }


    @Override
    public String toString() {
        return "MyMessage{" + "name='" + name + '\'' + "time=" + time + ", dataType='" + dataType + '\'' + ", data=" + "..." + ", textData='" + textData + '\'' + ", producer='" + producer + '\'' + ", topic='" + topic + '\'' + ", dataPath='" + dataPath + '\'' + '}';
    }
}
