module org.ismail.kafkaProducer {
    requires javafx.controls;
    requires javafx.fxml;
    requires javafx.graphics;

    requires kafka.clients;
    requires org.slf4j;
    requires org.jetbrains.annotations;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires kafka.json.serializer;
    opens org.ismail.kafkaProducer.utils to com.fasterxml.jackson.databind;

    opens org.ismail.kafkaProducer to javafx.fxml;
    exports org.ismail.kafkaProducer;
}