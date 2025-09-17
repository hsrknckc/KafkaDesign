package org.ismail.kafkamonitor;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;
import org.kordamp.bootstrapfx.BootstrapFX;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class HelloApplication extends Application {
    private HelloController helloController;
    Random random = new Random();

    @Override
    public void start(Stage stage) throws IOException {
        FXMLLoader fxmlLoader = new FXMLLoader(HelloApplication.class.getResource("hello-view.fxml"));
        Scene scene = new Scene(fxmlLoader.load());
        helloController =  fxmlLoader.getController();
        scene.getStylesheets().add(BootstrapFX.bootstrapFXStylesheet());
        stage.setTitle("Kafka Monitor");
        stage.setScene(scene);
        stage.setOnCloseRequest(event -> {
            Platform.exit();
            System.exit(0);
        });
        stage.show();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        if (helloController != null) {
            helloController.stopMonitor();
        }
    }

    public static void main(String[] args) {
        launch();
    }
}