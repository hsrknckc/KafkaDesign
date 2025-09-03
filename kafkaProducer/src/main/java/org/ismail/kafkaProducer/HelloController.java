package org.ismail.kafkaProducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.stage.FileChooser;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HelloController {
    @FXML
    private TextField topicName;
    @FXML
    private TextField topicName2;
    @FXML
    private TextField message;
    @FXML
    private Label status1;
    @FXML
    private Label status2;
    @FXML
    private ComboBox<String> topicBox;
    @FXML
    private ComboBox<String> topicBox2;

    private final FileChooser fileChooser = new FileChooser();

    private final ProducerDemo producerDemo = new ProducerDemo();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @FXML
    public void initialize() {
        topicBox.getItems().addAll("topic_metin", "topic_resim", "topic_ses");
        topicBox.setOnAction(event -> topicName.setText(topicBox.getValue()));

        topicBox2.getItems().addAll("topic_metin", "topic_resim", "topic_ses");
        topicBox2.setOnAction(event -> topicName2.setText(topicBox2.getValue()));
    }


    @FXML
    protected void sendString() throws JsonProcessingException {
        String topic = topicName.getText().trim();
        String msg = message.getText();
        producerDemo.sendStringMessage(topic, msg);
        status1.setText("Gönderildi!");
        scheduler.schedule(() -> Platform.runLater(() -> status1.setText("")), 1, TimeUnit.SECONDS);
    }

    File fileToSend;

    @FXML
    protected void sendFile() throws IOException {
        String topic = topicName2.getText().trim();
        String path = fileToSend.getAbsolutePath();
        producerDemo.sendFileMessage(topic, path);
        status2.setText("Gönderildi!");
        scheduler.schedule(() -> Platform.runLater(() -> status2.setText("")), 1, TimeUnit.SECONDS);

    }

    @FXML
    protected void selectFile() {
        fileToSend = fileChooser.showOpenDialog(null);
    }

}
