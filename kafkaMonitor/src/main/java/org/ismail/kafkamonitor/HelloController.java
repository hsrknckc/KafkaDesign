package org.ismail.kafkamonitor;

import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.XYChart;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.util.Duration;
import org.ismail.kafkamonitor.utils.MonitorListener;
import org.ismail.kafkamonitor.utils.MonitoredMessage;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class HelloController {
    @FXML private ListView<String> topicListView;
    private final ObservableList<String> topicList = FXCollections.observableArrayList();

    @FXML private TextField newTopicName, partitionCount, replicationFactor;
    @FXML private ComboBox<String> topicToDelete;
    @FXML private TextField aclCTopic, aclCUser, groupId;
    @FXML private ComboBox<String> permissionType;
    @FXML private Label status1;

    @FXML private LineChart<String, Number> messagesChart;
    private final XYChart.Series<String, Number> messagesSeries = new XYChart.Series<>();
    private final XYChart.Series<String, Number> consumeSeries = new XYChart.Series<>();

    @FXML private LineChart<String, Number> consumeChart;

    @FXML private LineChart<String, Number> memoryChart;
    private final XYChart.Series<String, Number> memorySeries = new XYChart.Series<>();

    @FXML private LineChart<String, Number> cpuChart;
    private final XYChart.Series<String, Number> cpuSeries = new XYChart.Series<>();

    @FXML private TableView<MonitoredMessage> messagesTable;
    @FXML private TableColumn<MonitoredMessage, Long> offsetColumn;
    @FXML private TableColumn<MonitoredMessage, String> producerColumn;
    @FXML private TableColumn<MonitoredMessage, String> timeColumn;
    @FXML private TableColumn<MonitoredMessage, String> topicColumn;
    @FXML private TableColumn<MonitoredMessage, String> readStatusColumn;
    @FXML private TableColumn<MonitoredMessage, String> dataColumn;
    @FXML private TableColumn<MonitoredMessage, String> produceTimeMsColumn;
    @FXML private TableColumn<MonitoredMessage, Integer> partitionColumn;

    private FilteredList<MonitoredMessage> filteredData;

    private final ObservableList<MonitoredMessage> monitorData = FXCollections.observableArrayList();
    private final AtomicReference<MonitorListener> currentListener = new AtomicReference<>();

    @FXML private ComboBox<String> topicToSee;
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    @FXML private TextArea clusterInfoArea;
    @FXML private TextArea brokerLatency;

    @FXML
    private void initialize() throws ExecutionException, InterruptedException {
        // PermissionType setup
        permissionType.getItems().addAll("Consumer", "Producer");
        permissionType.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if ("Consumer".equals(newVal)) {
                groupId.setVisible(true);
                groupId.setDisable(false);
            } else {
                groupId.setVisible(false);
                groupId.setDisable(true);
            }
        });

        topicListView.setItems(topicList);
        topicList.addAll(AdminOperations.listTopics());

        topicToSee.getItems().addAll(topicList.toArray(new String[0]));
        topicToDelete.getItems().addAll(topicList.toArray(new String[0]));

        clusterInfoArea.setText(AdminOperations.describeClusterInfo());

        // LineChart setup
        messagesChart.getData().add(messagesSeries);
        messagesChart.setLegendVisible(false);
        messagesSeries.setName("Messages/sec");

        consumeChart.getData().add(consumeSeries);
        consumeChart.setLegendVisible(false);
        consumeSeries.setName("Avg Latency (ms)");

        memoryChart.getData().add(memorySeries);
        memoryChart.setLegendVisible(false);
        memorySeries.setName("Memory");

        cpuChart.getData().add(cpuSeries);
        cpuChart.setLegendVisible(false);
        cpuSeries.setName("CPU");

        // TableView setup
        offsetColumn.setCellValueFactory(new PropertyValueFactory<>("offset"));
        producerColumn.setCellValueFactory(new PropertyValueFactory<>("producer"));
        timeColumn.setCellValueFactory(cell -> new SimpleStringProperty(cell.getValue().getTimestamp().format(formatter)));
        topicColumn.setCellValueFactory(new PropertyValueFactory<>("topic"));
        readStatusColumn.setCellValueFactory(cell ->
                new SimpleStringProperty(cell.getValue().getConsumerGroupsReadStatus().toString())
        );
        dataColumn.setCellValueFactory(new PropertyValueFactory<>("dataType"));
        produceTimeMsColumn.setCellValueFactory(new PropertyValueFactory<>("produceTimeMs"));
        partitionColumn.setCellValueFactory(new PropertyValueFactory<>("partition"));

        // FilteredList setup
        filteredData = new FilteredList<>(monitorData, p -> true);
        messagesTable.setItems(filteredData);

        // Producer filter
        ComboBox<String> producerFilterBox = new ComboBox<>();
        producerFilterBox.getItems().add("Tümü");
        producerFilterBox.setValue("Tümü");
        producerFilterBox.setMaxWidth(50);
        producerColumn.setGraphic(producerFilterBox);

        // Consumer filter
        ComboBox<String> consumerFilterBox = new ComboBox<>();
        consumerFilterBox.getItems().add("Tümü");
        consumerFilterBox.setValue("Tümü");
        consumerFilterBox.setMaxWidth(50);
        readStatusColumn.setGraphic(consumerFilterBox);

        // Filter listener
        producerFilterBox.valueProperty().addListener((obs, oldVal, newVal) -> applyFilters(producerFilterBox, consumerFilterBox));
        consumerFilterBox.valueProperty().addListener((obs, oldVal, newVal) -> applyFilters(producerFilterBox, consumerFilterBox));

        // Dinamik olarak producer ve consumer ekleme
        monitorData.addListener((javafx.collections.ListChangeListener<MonitoredMessage>) change -> {
            for (MonitoredMessage msg : monitorData) {
                // Producer
                if (msg.getProducer() != null && !msg.getProducer().isBlank() &&
                        !producerFilterBox.getItems().contains(msg.getProducer())) {
                    producerFilterBox.getItems().add(msg.getProducer());
                }
                // Consumer
                for (String consumer : msg.getConsumerGroupsReadStatus().keySet()) {
                    if (!consumerFilterBox.getItems().contains(consumer)) {
                        consumerFilterBox.getItems().add(consumer);
                    }
                }
            }
        });

        // Satır çift tıklama
        messagesTable.setRowFactory(tv -> {
            TableRow<MonitoredMessage> row = new TableRow<>();
            row.setOnMouseClicked(event -> {
                if (!row.isEmpty() && event.getClickCount() == 2) {
                    MonitoredMessage msg = row.getItem();

                    Stage dialog = new Stage();
                    dialog.initModality(Modality.APPLICATION_MODAL);
                    dialog.setTitle("Mesaj detayı");

                    VBox vbox = new VBox(10);
                    vbox.setPadding(new Insets(10));

                    TextArea textArea = new TextArea();
                    textArea.setEditable(false);
                    textArea.setWrapText(true);

                    String sb;
                    if ("str".equals(msg.getName())) {
                        sb = "File Name: " + msg.getName() + "\n" +
                                "Timestamp: " + msg.getTimestamp() + "\n" +
                                "Topic: " + msg.getTopic() + "\n" +
                                "Partition: " + msg.getPartition() + "\n" +
                                "Producer: " + msg.getProducer() + "\n" +
                                "Message type: " + msg.getDataType() + "\n" +
                                "Text data: " + msg.getTextData() + "\n";
                    } else {
                        sb = "File Name: " + msg.getName() + "\n" +
                                "Chunk: " + (msg.getChunkNumber() + 1) + "/" + msg.getTotalChunk() + "\n" +
                                "Timestamp: " + msg.getTimestamp() + "\n" +
                                "Topic: " + msg.getTopic() + "\n" +
                                "Partition: " + msg.getPartition() + "\n" +
                                "Producer: " + msg.getProducer() + "\n" +
                                "Message type: " + msg.getDataType() + "\n" +
                                "Byte Length: " + msg.getData().length + "\n";
                    }

                    textArea.setText(sb);
                    vbox.getChildren().add(textArea);

                    Scene scene = new Scene(vbox, 400, 300);
                    dialog.setScene(scene);
                    dialog.show();
                }
            });
            return row;
        });

        // Timeline ile sürekli yenileme
        Timeline refreshTimeline = new Timeline(new KeyFrame(Duration.seconds(5), e -> {
            refreshTable();
            updateMetrics();
        }));
        refreshTimeline.setCycleCount(Timeline.INDEFINITE);
        refreshTimeline.play();
    }

    private void applyFilters(ComboBox<String> producerFilterBox, ComboBox<String> consumerFilterBox) {
        String selectedProducer = producerFilterBox.getValue();
        String selectedConsumer = consumerFilterBox.getValue();

        filteredData.setPredicate(msg -> {
            boolean producerMatch = "Tümü".equals(selectedProducer) || selectedProducer.equals(msg.getProducer());
            boolean consumerMatch = true; // default
            if (!"Tümü".equals(selectedConsumer)) {
                Boolean hasRead = msg.getConsumerGroupsReadStatus().get(selectedConsumer);
                consumerMatch = hasRead != null && hasRead; // sadece okuyanlar gelsin
            }
            return producerMatch && consumerMatch;
        });
    }

    @FXML
    private void listenTopic() {
        String topic = topicToSee.getValue();
        if (topic != null && !topic.isBlank()) {
            stopMonitor();
            monitorData.clear();
            startMonitorListener(topic.trim());
        }
    }

    private void refreshTable() {
        Platform.runLater(() -> {
            for (MonitoredMessage monitoredMessage : monitorData) {
                try {
                    monitoredMessage.updateReadStatus(currentListener.get());
                } catch (Exception ignored) {}
            }
            messagesTable.refresh();
        });
    }

    private void updateMetrics() {
        Platform.runLater(() -> {
            String timeLabel = new SimpleDateFormat("HH:mm:ss").format(new Date());

            long messages = AdminOperations.producerSpeed();
            long consumedBytes = AdminOperations.consumerByteSpeed();
            double latency = AdminOperations.getBrokerRequestLatencyMs();
            DecimalFormat df = new DecimalFormat("#.##");

            long memoryUsage;
            double cpuUsage;
            try {
                memoryUsage = AdminOperations.getMemoryUsage();
                cpuUsage = AdminOperations.getCpuUsage();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            memorySeries.getData().add(new XYChart.Data<>(timeLabel, memoryUsage / 1024000));
            cpuSeries.getData().add(new XYChart.Data<>(timeLabel, cpuUsage));

            brokerLatency.setText("Broker gecikmesi: " + df.format(latency) + " ms");

            messagesSeries.getData().add(new XYChart.Data<>(timeLabel, messages));
            consumeSeries.getData().add(new XYChart.Data<>(timeLabel, consumedBytes));

            if (messagesSeries.getData().size() > 30) messagesSeries.getData().removeFirst();
            if (consumeSeries.getData().size() > 30) consumeSeries.getData().removeFirst();
            if (memorySeries.getData().size() > 30) memorySeries.getData().removeFirst();
        });
    }

    public void stopMonitor() {
        MonitorListener listener = currentListener.get();
        if (listener != null) listener.stop();
    }

    private void startMonitorListener(String topic) {
        MonitorListener listener = new MonitorListener(topic, msg -> Platform.runLater(() -> {
            monitorData.addFirst(msg);
            if (monitorData.size() > 100) monitorData.removeLast();
        }));
        currentListener.set(listener);
        Thread t = new Thread(listener);
        t.setDaemon(true);
        t.start();
    }

    @FXML
    protected void listTopicsList() throws ExecutionException, InterruptedException {
        topicList.clear();
        topicToSee.getItems().clear();
        topicToDelete.getItems().clear();

        topicList.addAll(AdminOperations.listTopics());
        topicToSee.getItems().addAll(topicList.toArray(new String[0]));
        topicToDelete.getItems().addAll(topicList.toArray(new String[0]));
    }

    @FXML
    protected void createTopic() {
        String result = AdminOperations.createTopic(newTopicName.getText(),
                Integer.parseInt(partitionCount.getText()),
                (short) Integer.parseInt(replicationFactor.getText()));
        status1.setText(result);
    }

    @FXML
    protected void deleteTopic() throws ExecutionException, InterruptedException {
        AdminOperations.deleteTopic(topicToDelete.getValue().trim());
    }

    @FXML
    protected void createAcls() {
        String perm = permissionType.getSelectionModel().getSelectedItem();
        if ("Producer".equals(perm)) {
            AdminOperations.createAcls(aclCTopic.getText(), aclCUser.getText());
        } else if ("Consumer".equals(perm)) {
            AdminOperations.createAcls(aclCTopic.getText(), aclCUser.getText(), groupId.getText());
        }
    }
}
