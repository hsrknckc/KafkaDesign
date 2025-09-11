module org.ismail.kafkaConsumer {
    requires javafx.controls;
    requires javafx.fxml;
    requires javafx.graphics;
    requires javafx.base;
    requires org.slf4j;
    requires kafka.clients;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires nanohttpd;
    requires logback.classic;
    requires logback.core;
    requires kafka.json.serializer;
    opens org.ismail.kafkaConsumer.utils to com.fasterxml.jackson.databind;

    opens org.ismail.kafkaConsumer to javafx.fxml;
    exports org.ismail.kafkaConsumer;
    exports org.ismail.kafkaConsumer.utils;
}