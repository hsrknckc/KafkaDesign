package org.ismail.kafkamonitor;

import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.XYChart;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.util.Duration;
import org.ismail.kafkamonitor.utils.MonitorListener;
import org.ismail.kafkamonitor.utils.MonitoredMessage;

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
    private final XYChart.Series<String, Number> requestSeries = new XYChart.Series<>();
    private final XYChart.Series<String, Number> latencySeries = new XYChart.Series<>();

    @FXML private LineChart<String, Number> latencyChart;

    @FXML private TableView<MonitoredMessage> messagesTable;
    @FXML private TableColumn<MonitoredMessage, Long> offsetColumn;
    @FXML private TableColumn<MonitoredMessage, String> producerColumn;
    @FXML private TableColumn<MonitoredMessage, String> timeColumn;
    @FXML private TableColumn<MonitoredMessage, String> topicColumn;
    @FXML private TableColumn<MonitoredMessage, String> readStatusColumn;

    private final ObservableList<MonitoredMessage> monitorData = FXCollections.observableArrayList();
    private final AtomicReference<MonitorListener> currentListener = new AtomicReference<>();

    @FXML private ComboBox<String> topicToSee;
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    @FXML private TextArea clusterInfoArea;


    @FXML
    private void initialize() throws ExecutionException, InterruptedException {
        // PermissionType setup
        permissionType.getItems().addAll("Consumer", "Producer");
        permissionType.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if("Consumer".equals(newVal)){
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
        messagesChart.getData().addAll(messagesSeries,requestSeries);
        messagesChart.setLegendVisible(false);
        messagesSeries.setName("Messages/sec");
        requestSeries.setName("Requests/sec");


        latencyChart.getData().add(latencySeries);
        latencyChart.setLegendVisible(false);
        latencySeries.setName("Avg Latency (ms)");


        // TableView setup
        offsetColumn.setCellValueFactory(new PropertyValueFactory<>("offset"));
        producerColumn.setCellValueFactory(new PropertyValueFactory<>("producer"));
        timeColumn.setCellValueFactory(cell -> new SimpleStringProperty(cell.getValue().getTimestamp().format(formatter)));
        topicColumn.setCellValueFactory(new PropertyValueFactory<>("topic"));
        readStatusColumn.setCellValueFactory(cell ->
                new SimpleStringProperty(cell.getValue().getConsumerGroupsReadStatus().toString())
        );
        messagesTable.setItems(monitorData);

        Timeline refreshTimeline = new Timeline(new KeyFrame(Duration.seconds(1), e -> {
            refreshTable();
            updateMetrics();
        }));
        refreshTimeline.setCycleCount(Timeline.INDEFINITE);
        refreshTimeline.play();
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
                }catch (Exception e){
                    //System.err.println(e.getMessage());
                }
            }
            messagesTable.refresh();
        });
    }

    private void updateMetrics(){
        Platform.runLater(()->{
            String timeLabel = new SimpleDateFormat("HH:mm:ss").format(new Date());

            long messages = AdminOperations.producerSpeed();
            long requests = AdminOperations.getBrokerRequestPerSec();
            double latency = AdminOperations.getBrokerRequestLatencyMs();

            messagesSeries.getData().add(new XYChart.Data<>(timeLabel, messages));
            //requestSeries.getData().add(new XYChart.Data<>(timeLabel, requests));
            latencySeries.getData().add(new XYChart.Data<>(timeLabel, latency));

            // Limit chart points to last 30 entries
            if (messagesSeries.getData().size() > 30) messagesSeries.getData().removeFirst();
            if (requestSeries.getData().size() > 30) requestSeries.getData().removeFirst();
            if (latencySeries.getData().size() > 30) latencySeries.getData().removeFirst();
        });
    }


    public void stopMonitor(){
        MonitorListener listener = currentListener.get();
        if(listener != null){
            listener.stop();
        }
    }

    private void startMonitorListener(String topic) {
        // Yeni listener callback ile kuruluyor
        MonitorListener listener = new MonitorListener(topic, msg -> Platform.runLater(() -> monitorData.add(msg)));

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
        if("Producer".equals(perm)){
            AdminOperations.createAcls(aclCTopic.getText(), aclCUser.getText());
        } else if("Consumer".equals(perm)){
            AdminOperations.createAcls(aclCTopic.getText(), aclCUser.getText(), groupId.getText());
        }
    }
}
