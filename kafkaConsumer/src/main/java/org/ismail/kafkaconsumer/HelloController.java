package org.ismail.kafkaconsumer;

import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.VBox;

import java.io.ByteArrayInputStream;

public class HelloController {
    @FXML
    private TextField topic;
    @FXML
    private TextArea messageArea;
    @FXML
    private VBox fileArea;
    @FXML
    private Label status;
    @FXML
    private ComboBox<String> myComboBox;

    String fileType = "";
    @FXML
    public void initialize(){
        myComboBox.getItems().addAll("text","image");
        myComboBox.setOnAction(event -> {
            fileType = myComboBox.getValue();
        });
    }

    private final ConsumerDemo consumerDemo = new ConsumerDemo();
    public void shutdown(){
        consumerDemo.close();
    }
    @FXML
    protected void previewMessages(){
        String topicName = topic.getText();

        status.setText("Durum: Dinleniyor");
        if(fileType.equals("text")) {
            Task<Void> kafkaTask = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    consumerDemo.readStringMessages(topicName, message -> {
                        Platform.runLater(() -> {
                            messageArea.appendText(message + "\n");
                        });
                    });
                    return null;
                }
            };
            Thread thread = new Thread(kafkaTask);
            thread.setDaemon(true);
            thread.start();
        }else if(fileType.equals("image")) {
            Task<Void> kafkaTask2 = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    consumerDemo.readFileMessages(topicName, message -> {
                        Platform.runLater(() -> {
                            Image image = new Image(new ByteArrayInputStream(message));
                            ImageView imageView = new ImageView(image);
                            imageView.setFitWidth(600);
                            fileArea.getChildren().add(imageView);
                        });
                    });
                    return null;
                }
            };
            Thread thread2 = new Thread(kafkaTask2);
            thread2.setDaemon(true);
            thread2.start();
        }
    }
}