module org.ismail.kafkaconsumer {
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

    opens org.ismail.kafkaconsumer to javafx.fxml;
    exports org.ismail.kafkaconsumer;
}