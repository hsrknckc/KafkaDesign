package org.ismail.kafkaConsumer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import javafx.application.HostServices;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.geometry.Insets;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;
import org.ismail.kafkaConsumer.utils.BasicHttpServer;
import org.ismail.kafkaConsumer.utils.LogCatcher;
import org.ismail.kafkaConsumer.utils.MyMessage;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicReference;

public class HelloController {
    @FXML
    private TextField topic;
    @FXML
    private ListView<VBox> fileArea;
    @FXML
    private Label status;
    @FXML
    private ComboBox<String> topicBox;
    final ObservableList<VBox> messages = FXCollections.observableArrayList();
    private String topicName;

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(HelloController.class);

    private BasicHttpServer server;

    @FXML
    public void initialize() {
        topicBox.getItems().addAll("topic_metin", "topic_resim", "topic_ses");
        topicBox.setOnAction(event -> topic.setText(topicBox.getValue()));
        fileArea.setItems(messages);

        try {
            File downloadDir = new File("downloads");
            if (!downloadDir.exists()) //noinspection ResultOfMethodCallIgnored
                downloadDir.mkdirs();
            server = new BasicHttpServer(8080, downloadDir);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        Logger log = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        log.setLevel(Level.INFO);
        LogCatcher catcher = new LogCatcher("Setting offset for partition", () -> status.setText("Durum: Dinleniyor -> " + topicName));
        catcher.start();
        log.addAppender(catcher);
    }

    private HostServices hostServices;

    public void setHostServices(HostServices hostServices) {
        this.hostServices = hostServices;
    }


    private void openInBrowser(String filePath) {
        if (hostServices != null) {
            hostServices.showDocument(filePath);
        }
    }


    private final AtomicReference<TopicListener> currentListener = new AtomicReference<>();




    @FXML
    protected void previewMessages() {
        topicName = topic.getText().trim();
        if (topicName.isEmpty()) return;



        TopicListener oldListener = currentListener.getAndSet(null);
        if (oldListener != null) oldListener.stop();

        status.setText("Lütfen bekleyin...");
        fileArea.getItems().clear();

        TopicListener listener = new TopicListener(topicName, message -> Platform.runLater(() -> displayMessage(message)));
        currentListener.set(listener);
        Thread t = new Thread(listener);
        t.setDaemon(true);
        t.start();
    }

    private void displayMessage(MyMessage message) {
        VBox tekMsg = new VBox();
        Label infoLabel = new Label("Dosya adı: " + message.getName() + ", Tür: " + message.getDataType() + "\n" + "Zaman: " + message.getTime().format(formatter));
        infoLabel.setWrapText(true);
        tekMsg.setStyle("-fx-border-color: gray; " + "-fx-border-width: 2; " + "-fx-border-radius: 5; " + "-fx-padding: 2;");
        VBox.setMargin(tekMsg, new Insets(0, 0, 2, 0));
        tekMsg.getChildren().add(infoLabel);

        if (!message.getDataType().equals("string")) {
            try {
                String folder = "downloads";
                Files.createDirectories(Paths.get(folder));
                String filePath = folder + File.separator + message.getName();

                Files.write(Paths.get(filePath), message.getData());
                Button downloadButton = new Button("İndir");


                String url = "http://localhost:8080/" + message.getName();
                downloadButton.setOnAction(event -> openInBrowser(url));
                tekMsg.getChildren().add(downloadButton);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            Label msgLabel1 = new Label(message.getTextData());
            tekMsg.getChildren().add(msgLabel1);
        }
        messages.add(tekMsg);
    }

    public void shutdown() throws IOException {
        deleteDownloads();
        TopicListener listener = currentListener.get();
        if (listener != null) listener.stop();
        server.stop();
    }

    public void deleteDownloads() throws IOException {
        Path downloads = Paths.get("downloads");
        //noinspection resource
        Files.walk(downloads)
                .filter(path -> !path.equals(downloads))
                .map(Path::toFile)
                .forEach(file -> {
                    if (!file.delete()) {
                        logger.error("Silinemedi");
                    }
                });

    }
}