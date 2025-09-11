module org.ismail.kafkamonitor {
    requires javafx.controls;
    requires javafx.fxml;
    requires javafx.web;

    requires org.controlsfx.controls;
    requires com.dlsc.formsfx;
    requires net.synedra.validatorfx;
    requires org.kordamp.ikonli.javafx;
    requires org.kordamp.bootstrapfx.core;
    requires eu.hansolo.tilesfx;
    requires kafka.clients;
    requires java.logging;
    requires java.management;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires com.fasterxml.jackson.databind;
    requires org.slf4j;
    requires kafka.json.serializer;
    requires logback.classic;
    requires jdk.management;

    opens org.ismail.kafkamonitor to javafx.fxml;
    exports org.ismail.kafkamonitor;
    opens org.ismail.kafkamonitor.utils to com.fasterxml.jackson.databind, javafx.base;
}