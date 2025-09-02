module org.ismail.kafkaProducer {
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
    requires kafka.clients;
    requires org.slf4j;
    requires java.desktop;
    requires annotations;
    requires org.json;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jsr310;
    opens org.ismail.kafkaProducer.utils to com.fasterxml.jackson.databind;

    opens org.ismail.kafkaProducer to javafx.fxml;
    exports org.ismail.kafkaProducer;
}