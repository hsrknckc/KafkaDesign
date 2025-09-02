package org.ismail.kafkaConsumer;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import javafx.scene.media.MediaView;
import javafx.util.Duration;
import org.ismail.kafkaConsumer.utils.MyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicReference;

public class HelloController {
    @FXML
    private TextField topic;
    @FXML
    private VBox fileArea;
    @FXML
    private Label status;

    @FXML
    private ScrollPane scrollPane;

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
    private final Logger logger = LoggerFactory.getLogger(HelloController.class);

    private final AtomicReference<TopicListener> currentListener = new AtomicReference<>();

    @FXML
    protected void previewMessages() {
        String topicName = topic.getText().trim();
        if (topicName.isEmpty()) return;

        TopicListener oldListener = currentListener.getAndSet(null);
        if (oldListener != null) oldListener.stop();

        fileArea.getChildren().clear();
        status.setText("Durum: Dinleniyor " + topicName);

        TopicListener listener = new TopicListener(topicName, message -> Platform.runLater(() -> displayMessage(message)));
        currentListener.set(listener);
        Thread t = new Thread(listener);
        t.setDaemon(true);
        t.start();
    }

    private void displayMessage(MyMessage message) {
        Label infoLabel = new Label(
                "Dosya adı: " + message.getName() +
                        ", Tür: " + message.getDataType() +
                        ", Zaman: " + message.getTime().format(formatter));
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
                    HBox controls = new HBox(10, playButton, pauseButton);

                    Slider progress = new Slider();
                    progress.setMin(0);
                    progress.setMax(100);
                    progress.setValue(0);
                    progress.setPrefWidth(400);

                    VBox audioBox = new VBox(5, controls, progress);
                    fileArea.getChildren().add(audioBox);
                    playButton.setOnAction(event -> mediaPlayer.play());
                    pauseButton.setOnAction(event -> mediaPlayer.pause());

                    mediaPlayer.currentTimeProperty().addListener((obs, oldTime, newTime) -> {
                        if (!progress.isValueChanging()) {
                            Duration total = mediaPlayer.getTotalDuration();
                            if (total != null && total.toMillis() > 0) {
                                progress.setValue(newTime.toMillis() / total.toMillis() * 100);
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

                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
                break;
        }
        scrollPane.layout();
        scrollPane.setVvalue(1.0);
    }

    public void shutdown() {
        TopicListener listener = currentListener.get();
        if (listener != null) listener.stop();
    }
}