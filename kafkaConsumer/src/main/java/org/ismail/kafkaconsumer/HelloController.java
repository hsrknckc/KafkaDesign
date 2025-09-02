package org.ismail.kafkaconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.media.*;
import javafx.util.Duration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;

public class HelloController {
    @FXML
    private TextField topic;
    @FXML
    private VBox fileArea;
    @FXML
    private Label status;

    @FXML
    private ScrollPane scrollPane;

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");


    private final ConsumerDemo consumerDemo = new ConsumerDemo();
    public void shutdown(){
        consumerDemo.close();
    }
    @FXML
    protected void previewMessages() throws JsonProcessingException {
        String topicName = topic.getText();

        status.setText("Durum: Dinleniyor");

        Task<Void> kafkaTask = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    consumerDemo.readFileMessages(topicName, message -> {
                        Platform.runLater(() -> {
                            Label infoLabel = new Label("Dosya adı: " + message.getName() + ", Tür: " + message.getDataType() + ", Zaman: " + message.getTime().format(formatter));
                            fileArea.getChildren().add(infoLabel);
                            switch (message.getDataType()) {
                                case "string":
                                    Label msgLabel1 = new Label(message.getTextData());
                                    fileArea.getChildren().add(msgLabel1);
                                    break;
                                case "text":
                                    String textContent = new String(message.getData(), StandardCharsets.UTF_8);
                                    TextArea textArea = new TextArea(textContent);
                                    textArea.setWrapText(true);
                                    textArea.setEditable(false);
                                    textArea.setPrefWidth(400);
                                    fileArea.getChildren().add(textArea);
                                    break;
                                case "image":
                                    Image image = new Image(new ByteArrayInputStream(message.getData()));
                                    ImageView imageView = new ImageView(image);
                                    imageView.setPreserveRatio(true);
                                    imageView.setFitWidth(400);
                                    fileArea.getChildren().add(imageView);
                                    break;
                                case "audio":
                                    try {
                                        String tempFile = "temp_audio.mp3";
                                        Files.write(Paths.get(tempFile), message.getData());
                                        Media media = new Media(new File(tempFile).toURI().toString());
                                        MediaPlayer mediaPlayer = new MediaPlayer(media);

                                        MediaView mediaView = new MediaView(mediaPlayer);
                                        fileArea.getChildren().add(mediaView);

                                        Button playButton = new Button("Play");
                                        Button pauseButton = new Button("Pause");
                                        HBox controls =  new HBox(10,playButton,pauseButton);

                                        Slider progress = new Slider();
                                        progress.setMin(0);
                                        progress.setMax(100);
                                        progress.setValue(0);
                                        progress.setPrefWidth(400);

                                        VBox audioBox = new VBox(5,controls,progress);
                                        fileArea.getChildren().add(audioBox);
                                        playButton.setOnAction(event -> mediaPlayer.play());
                                        pauseButton.setOnAction(event -> mediaPlayer.pause());

                                        mediaPlayer.currentTimeProperty().addListener((obs, oldTime, newTime) -> {
                                            if(!progress.isValueChanging()){
                                                Duration total = mediaPlayer.getTotalDuration();
                                                if(total!=null && total.toMillis() > 0){
                                                    progress.setValue(newTime.toMillis()/total.toMillis() * 100);
                                                }
                                            }
                                        });

                                        progress.valueChangingProperty().addListener((obs, wasChanging, isChanging) -> {
                                            if (!isChanging) {
                                                Duration total = mediaPlayer.getTotalDuration();
                                                if (total != null) {
                                                    mediaPlayer.seek(total.multiply(progress.getValue() / 100.0));
                                                }
                                            }
                                        });

                                    }catch (Exception e){
                                        e.printStackTrace();
                                    }
                                    break;
                            }
                            scrollPane.layout();
                            scrollPane.setVvalue(1.0);
                        });
                    });
                    return null;
                }
            };
            Thread thread = new Thread(kafkaTask);
            thread.setDaemon(true);
            thread.start();














//        if(fileType.equals("text")) {
//            Task<Void> kafkaTask = new Task<>() {
//                @Override
//                protected Void call() throws Exception {
//                    consumerDemo.readFileMessages(topicName, message -> {
//                        Platform.runLater(() -> {
//                            Label infoLabel = new Label("Tür: " + message.getDataType() + " Zaman: " + message.getTime().format(formatter));
//                            fileArea.getChildren().add(infoLabel);
//                            Label msgLabel = new Label(message.getTextData());
//                            fileArea.getChildren().add(msgLabel);
//                            scrollPane.layout();
//                            scrollPane.setVvalue(1.0);
//                        });
//                    });
//                    return null;
//                }
//            };
//            Thread thread = new Thread(kafkaTask);
//            thread.setDaemon(true);
//            thread.start();
//        }else if(fileType.equals("image")) {
//            Task<Void> kafkaTask2 = new Task<>() {
//                @Override
//                protected Void call() throws Exception {
//                    consumerDemo.readFileMessages(topicName, message -> {
//                        Platform.runLater(() -> {
//                            Label infoLabel = new Label("Tür: " + message.getDataType() + " Zaman: " + message.getTime().format(formatter));
//                            fileArea.getChildren().add(infoLabel);
//                            Image image = new Image(new ByteArrayInputStream(message.getData()));
//                            ImageView imageView = new ImageView(image);
//                            imageView.setPreserveRatio(true);
//                            imageView.setFitWidth(400);
//                            fileArea.getChildren().add(imageView);
//                            scrollPane.layout();
//                            scrollPane.setVvalue(1.0);
//                        });
//                    });
//                    return null;
//                }
//            };
//            Thread thread2 = new Thread(kafkaTask2);
//            thread2.setDaemon(true);
//            thread2.start();
//        } else if (fileType.equals("audio")) {
//            Task<Void> kafkaTask3 = new Task<>() {
//                @Override
//                protected Void call() throws Exception {
//                    consumerDemo.readFileMessages(topicName, message -> {
//                        Platform.runLater(() -> {
//                            try{
//                                Label infoLabel = new Label("Tür: " + message.getDataType() + " Zaman: " + message.getTime().format(formatter));
//                                fileArea.getChildren().add(infoLabel);
//                                String tempFile="temp_audio.mp3";
//                                Files.write(Paths.get(tempFile), message.getData());
//                                Media media = new Media(new File(tempFile).toURI().toString());
//                                MediaPlayer mediaPlayer = new MediaPlayer(media);
//                                MediaView mediaView = new MediaView(mediaPlayer);
//                                fileArea.getChildren().add(mediaView);
//
//                                scrollPane.layout();
//                                scrollPane.setVvalue(1.0);
//
//                            }catch (IOException e){
//                                e.printStackTrace();
//                            }
//                        });
//                    });
//                    return null;
//                }
//            };
//            Thread thread3 = new Thread(kafkaTask3);
//            thread3.setDaemon(true);
//            thread3.start();
//
//        }
    }
}