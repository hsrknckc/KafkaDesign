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
    private final ProducerDemo producerDemo = new ProducerDemo();

    @FXML private TextField topicName;
    @FXML private TextField message;
    @FXML private ComboBox<String> topicBox;
    @FXML private ListView<String> stringList;
    private final ObservableList<String> stringObservableList = FXCollections.observableArrayList();

    @FXML private TextField topicName2;
    @FXML private ComboBox<String> topicBox2;
    @FXML private ListView<String> fileList;
    private File fileToSend;
    private final ObservableList<String> fileObservableList = FXCollections.observableArrayList();

    private final FileChooser fileChooser = new FileChooser();

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
            public void onSuccess(long offset) {
                Platform.runLater(() -> stringObservableList.addFirst("Başarıyla gönderildi! Offset: " + offset));
            }

            @Override
            public void onFail(String error) {
                Platform.runLater(() -> stringObservableList.addFirst("Gönderilemedi: " + error));
            }
        });
    }

    @FXML
    protected void sendFile() throws IOException {
        String topic = topicName2.getText().trim();
        String path = fileToSend.getAbsolutePath();
        producerDemo.sendFileMessage(topic, path,  new ProducerDemo.AckCallback() {
            @Override
            public void onSuccess(long offset) {
                Platform.runLater(() -> fileObservableList.addFirst("Chunk başarıyla gönderildi! Offset: " + offset));
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
