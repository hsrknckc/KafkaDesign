package org.ismail.kafkaProducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.stage.FileChooser;

import java.io.File;
import java.io.IOException;

public class HelloController {
    @FXML
    private TextField topicName;
    @FXML
    private TextField topicName2;
    @FXML
    private TextField message;
    @FXML
    private ComboBox<String> topicBox;
    @FXML
    private ComboBox<String> topicBox2;
    @FXML
    private ListView<String> stringList;
    @FXML
    private ListView<String> fileList;


    private final FileChooser fileChooser = new FileChooser();

    private final ProducerDemo producerDemo = new ProducerDemo();


    private final ObservableList<String> stringObservableList = FXCollections.observableArrayList();
    private final ObservableList<String> fileObservableList = FXCollections.observableArrayList();



    @FXML
    public void initialize() {
        topicBox.getItems().addAll("topic_metin", "topic_resim_3", "topic_ses_3");
        topicBox.setOnAction(event -> topicName.setText(topicBox.getValue()));

        topicBox2.getItems().addAll("topic_metin", "topic_resim_3", "topic_ses_3");
        topicBox2.setOnAction(event -> topicName2.setText(topicBox2.getValue()));

        stringList.setItems(stringObservableList);
        fileList.setItems(fileObservableList);
    }


    @FXML
    protected void sendString() throws JsonProcessingException {
        String topic = topicName.getText().trim();
        String msg = message.getText();
        producerDemo.sendStringMessage(topic, msg, new ProducerDemo.AckCallback() {
            @Override
            public void onSuccess(long offset,long produceTime) {
                Platform.runLater(() -> stringObservableList.addFirst("Başarıyla gönderildi! Offset: " + offset + "; " + produceTime + " ms"));
            }

            @Override
            public void onFail(String error) {
                Platform.runLater(() -> stringObservableList.addFirst("Gönderilemedi: " + error));
            }
        });
    }

    File fileToSend;

    @FXML
    protected void sendFile() throws IOException {
        String topic = topicName2.getText().trim();
        String path = fileToSend.getAbsolutePath();
        producerDemo.sendFileMessage(topic, path,  new ProducerDemo.AckCallback() {
            @Override
            public void onSuccess(long offset, long produceTime) {
                Platform.runLater(() -> fileObservableList.addFirst("Chunk başarıyla gönderildi! Offset: " + offset + "; " + produceTime + " ms"));
            }
            @Override
            public void onFail(String error) {
                Platform.runLater(() -> fileObservableList.addFirst("Gönderilemedi: " + error));
            }
        });
    }

    @FXML
    protected void selectFile() {
        fileToSend = fileChooser.showOpenDialog(null);
    }

}
