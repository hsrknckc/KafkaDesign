package org.ismail.kafkaProducer.utils;

import java.time.LocalDateTime;
import java.util.Date;

public class MyMessage {
    public LocalDateTime time;
    public String dataType;
    public byte[] data;
    public String textData;
    public String producer;
    public String topic;
    public String dataPath;


    public MyMessage(LocalDateTime time, String dataType, byte[] data, String producer, String topic) {
        this.time = time;
        this.dataType = dataType;
        this.data = data;
        this.producer = producer;
        this.topic = topic;
    }

    public MyMessage(LocalDateTime time, String dataType, String dataPath, String producer, String topic) {
        this.time = time;
        this.dataType = dataType;
        this.dataPath = dataPath;
        this.producer = producer;
        this.topic = topic;
    }

    public MyMessage(LocalDateTime time, String textData, String producer, String topic) {
        this.time = time;
        this.dataType = "string";
        this.textData = textData;
        this.producer = producer;
        this.topic = topic;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public LocalDateTime getTime() {
        return time;
    }

    public void setTime(LocalDateTime time) {
        this.time = time;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getProducer() {
        return producer;
    }

    public void setProducer(String producer) {
        this.producer = producer;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }


}
