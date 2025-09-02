package org.ismail.kafkaProducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import org.ismail.kafkaProducer.utils.Utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class HelloController {
    @FXML private TextField topicName;
    @FXML private TextField topicName2;
    @FXML private TextField message;
    @FXML private VBox previewBox;
    @FXML private Label status1;
    @FXML private Label status2;

    private FileChooser fileChooser = new FileChooser();

    private final ProducerDemo producerDemo = new ProducerDemo();

    // Bölüm 1
    @FXML
    protected void sendString() throws JsonProcessingException {
        String topic = topicName.getText();
        String msg = message.getText();
        producerDemo.sendStringMessage(topic,msg);
        status1.setText("Gönderildi!");
    }

    File fileToSend;
    // Bölüm 2
    @FXML
    protected void sendFile() throws IOException {
        String topic = topicName2.getText();
        String path = fileToSend.getAbsolutePath();
        producerDemo.sendFileMessage(topic,path);
        status2.setText("Gönderildi!");
    }

    @FXML
    protected void selectFile() throws IOException {
        fileToSend = fileChooser.showOpenDialog(null);
        String fileType = Utilities.getFileType(fileToSend.getAbsolutePath());
        previewBox.getChildren().clear();
        TextArea fileTypeLabel = new TextArea("Dosya tipi: " + fileType);
        fileTypeLabel.setEditable(false);
        previewBox.getChildren().add(fileTypeLabel);
        switch (fileType) {
            case "text":
                TextArea textArea = new TextArea(fileToSend.toString());
                textArea.setEditable(false);
                previewBox.getChildren().add(textArea);
                break;

            case "image":
                Image image = new Image(new FileInputStream(fileToSend));
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
