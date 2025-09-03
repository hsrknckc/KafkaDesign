package org.ismail.kafkaProducer;

import java.io.IOException;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class HelloApplication extends Application {
    @Override
    public void start(Stage stage) throws IOException {
        FXMLLoader fxmlLoader = new FXMLLoader(HelloApplication.class.getResource("hello-view.fxml"));
        ProducerDemo producerDemo = new ProducerDemo();
        Scene scene = new Scene(fxmlLoader.load());
        stage.setTitle("Kafka Producer Client");
        stage.setScene(scene);
        stage.setOnCloseRequest(event -> producerDemo.close());
        stage.show();
    }

    public static void main(String[] args) {
        launch();
    }
}