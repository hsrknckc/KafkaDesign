package org.ismail.kafkaProducer;

import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.VBox;

import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class HelloController {
    @FXML private TextField topicName;
    @FXML private TextField topicName2;
    @FXML private TextField message2;
    @FXML private TextField message;
    @FXML private VBox previewBox;
    @FXML private Label status1;
    @FXML private Label status2;

    private final ProducerDemo producerDemo = new ProducerDemo();

    // Bölüm 1
    @FXML
    protected void sendString() {
        String topic = topicName.getText();
        String msg = message.getText();
        producerDemo.sendStringMessage(topic,msg);
        status1.setText("Gönderildi!");
    }

    // Bölüm 2
    @FXML
    protected void sendFile() throws IOException {
        String topic = topicName2.getText();
        String path = message2.getText();
        producerDemo.sendFileMessage(topic,path);
        status2.setText("Gönderildi!");
    }


    // Bölüm 3
    @FXML
    protected void previewFile() throws IOException {
        String path = message2.getText();
        String fileType = producerDemo.getFileType(path);
        previewBox.getChildren().clear();
        switch (fileType) {
            case "text":
                String content = Files.readString(Path.of(path));
                TextArea textArea = new TextArea(content);
                textArea.setEditable(false);
                previewBox.getChildren().add(textArea);
                break;

            case "image":
                Image image = new Image(new FileInputStream(path));
                ImageView imageView = new ImageView(image);
                imageView.setFitWidth(600);
                imageView.setPreserveRatio(true);
                previewBox.getChildren().add(imageView);
                break;

            default:
                TextArea infoArea = new TextArea("Dosya türü:"+ fileType);
                infoArea.setEditable(false);
                previewBox.getChildren().add(infoArea);
                break;

        }
    }
}
