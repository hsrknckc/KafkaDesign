package org.ismail.kafkaconsumer;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;
import java.io.IOException;

public class HelloApplication extends Application {
    @Override
    public void start(Stage stage) throws IOException {
        FXMLLoader fxmlLoader = new FXMLLoader(HelloApplication.class.getResource("hello-view.fxml"));

        HelloController controller = new HelloController();
        Scene scene = new Scene(fxmlLoader.load(), 900, 600);
        stage.setTitle("Kafka Consumer Client");
        stage.setScene(scene);
        stage.setOnCloseRequest(event ->{
            controller.shutdown();
        });
        stage.show();
    }

    public static void main(String[] args) {
        launch();
    }
}