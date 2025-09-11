package org.ismail.kafkaConsumer.utils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

@SuppressWarnings("ALL")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY, setterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY, isGetterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
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


    public MyMessage() {
    }

    public MyMessage(String name, LocalDateTime time, String dataType, byte[] data, String producer, String topic,String fileId, int chunkNumber, int totalChunk) {
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

    @JsonProperty("time")
    public LocalDateTime getTime() {
        return time;
    }

    public void setTime(LocalDateTime time) {
        this.time = time;
    }

    @JsonProperty("dataType")
    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    @JsonProperty("data")
    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @JsonProperty("textData")
    public String getTextData() {
        return textData;
    }

    public void setTextData(String textData) {
        this.textData = textData;
    }

    @JsonProperty("dataPath")
    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    @JsonProperty("producer")
    public String getProducer() {
        return producer;
    }

    public void setProducer(String producer) {
        this.producer = producer;
    }

    @JsonProperty("topic")
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("chunkNumber")
    public int getChunkNumber() {
        return chunkNumber;
    }

    public void setChunkNumber(int chunkNumber) {
        this.chunkNumber = chunkNumber;
    }

    @JsonProperty("totalChunk")
    public int getTotalChunk() {
        return totalChunk;
    }

    public void setTotalChunk(int totalChunk) {
        this.totalChunk = totalChunk;
    }

    @JsonProperty("fileId")
    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    @JsonProperty("produceTime")
    public long getProduceTime() {
        return produceTime;
    }

    public void setProduceTime(long produceTime) {
        this.produceTime = produceTime;
    }

    @Override
    public String toString() {
        return "MyMessage{" + "name='" + name + '\'' + "time=" + time + ", dataType='" + dataType + '\'' + ", data=" + "..." + ", textData='" + textData + '\'' + ", producer='" + producer + '\'' + ", topic='" + topic + '\'' + ", dataPath='" + dataPath + '\'' + '}';
    }
}
