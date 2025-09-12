module org.ismail.kafkamonitor {
    requires javafx.controls;
    requires javafx.fxml;
    requires javafx.graphics;
    requires javafx.base;

    requires org.kordamp.bootstrapfx.core;
    requires kafka.clients;
    requires java.management;
    requires org.slf4j;
    requires kafka.json.serializer;

    opens org.ismail.kafkamonitor to javafx.fxml;
    exports org.ismail.kafkamonitor;
    exports org.ismail.kafkamonitor.utils;
    opens org.ismail.kafkamonitor.utils to com.fasterxml.jackson.databind, javafx.base;
}