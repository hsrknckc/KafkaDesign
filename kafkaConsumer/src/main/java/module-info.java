module org.ismail.kafkaConsumer {
    requires javafx.controls;
    requires javafx.fxml;
    requires javafx.web;

    requires org.controlsfx.controls;
    requires com.dlsc.formsfx;
    requires net.synedra.validatorfx;
    requires org.kordamp.ikonli.javafx;
    requires org.kordamp.bootstrapfx.core;
    requires eu.hansolo.tilesfx;
    requires com.almasb.fxgl.all;
    requires org.slf4j;
    requires kafka.clients;
    requires javafx.media;
    requires org.json;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jsr310;
    opens org.ismail.kafkaConsumer.utils to com.fasterxml.jackson.databind;

    opens org.ismail.kafkaConsumer to javafx.fxml;
    exports org.ismail.kafkaConsumer;
}