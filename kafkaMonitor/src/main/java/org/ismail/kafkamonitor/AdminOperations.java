package org.ismail.kafkamonitor;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.ismail.kafkamonitor.config.Props;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminOperations {
    private static final AdminClient adminClient = AdminClient.create(new Props().properties);

    private static long prevMessageCount = 0;
    private static long prevRequestCount = 0;
    private static long prevConsumeCount = 0;

    public static Set<String> listTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        describeClusterInfo();
        printConsumerLag(Props.username);
        return listTopicsResult.names().get();
    }

    public static long producerSpeed() {
        try {
            JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
            JMXConnector jmxc= JMXConnectorFactory.connect(jmxServiceURL,null);
            MBeanServerConnection mbs = jmxc.getMBeanServerConnection();

            ObjectName name = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec");
            Object value = mbs.getAttribute(name,"Count");
            long currentCount = (Long)value;
            long delta = currentCount - prevMessageCount;
            prevMessageCount = currentCount;
            jmxc.close();
            return delta;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return 0;
        }
    }

    public static long consumerByteSpeed() {
        try {
            JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
            JMXConnector jmxc= JMXConnectorFactory.connect(jmxServiceURL,null);
            MBeanServerConnection mbs = jmxc.getMBeanServerConnection();

            ObjectName name = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec");
            Object value = mbs.getAttribute(name,"Count");
            long currentCount = (Long)value;
            long delta = currentCount - prevConsumeCount;
            prevConsumeCount = currentCount;
            jmxc.close();
            return delta;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return 0;
        }
    }

    public static String describeClusterInfo() throws InterruptedException, ExecutionException {
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        StringBuilder sb = new StringBuilder();

        String clusterId= describeClusterResult.clusterId().get();
        Node controller = describeClusterResult.controller().get();
        Collection<Node> nodes = describeClusterResult.nodes().get();
        System.out.println("Cluster ID: " + clusterId);
        System.out.println("Controller: " + controller);

        sb.append("Cluster ID: ").append(clusterId).append("\n");
        sb.append("Controller: ").append(controller).append("\n");

        for (Node node : nodes) {
            System.out.println("Broker: " + node.id() + " - " + node.host() + ":" + node.port());
            sb.append("Broker: ").append(node.id()).append(" - ").append(node.host()).append(":").append(node.port()).append("\n");
        }
        int urp = adminClient.describeCluster().nodes().get().size();
        System.out.println("Active Brokers: " + urp);
        sb.append("Active Brokers: ").append(urp).append("\n");

        return sb.toString();
    }

    public static void printConsumerLag(String groupId) throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsResult.partitionsToOffsetAndMetadata().get();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long committed = entry.getValue().offset();

            // endOffset almak için consumer veya adminClient ile
            ListOffsetsResult listOffsets = adminClient.listOffsets(
                    Map.of(tp, OffsetSpec.latest())
            );
            long endOffset = listOffsets.partitionResult(tp).get().offset();
            long lag = endOffset - committed;

            System.out.println("Topic: " + tp.topic() + ", Partition: " + tp.partition() + ", Lag: " + lag);
        }
    }

    public static String createTopic(String topicName,int partitions,short replicationFactor) {
        Collection<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(new NewTopic(topicName, partitions, replicationFactor));
        try {
            CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
            createTopicsResult.all().get();
            return "Topic oluşturuldu";
        }catch (Exception e) {
            String msg = e.getMessage();
            if(msg!=null && msg.contains(":")){
                msg = msg.substring(msg.lastIndexOf(":") +1).trim();
            }
            return msg;
        }
    }

    public static void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        Collection<String> topics = new ArrayList<>();
        topics.add(topicName);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);

        System.out.println(deleteTopicsResult.all().get());
    }

    public static void createAcls(String topicName, String user){

        AclBinding ac1 = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC,topicName,PatternType.LITERAL),
                new AccessControlEntry("User:"+user,"*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
        );
        AclBinding ac2 = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC,topicName,PatternType.LITERAL),
                new AccessControlEntry("User:"+user,"*", AclOperation.WRITE, AclPermissionType.ALLOW)
        );

        adminClient.createAcls(Arrays.asList(ac1,ac2));
        System.out.println("Producer permissions has given to user->" + user + ", topic-> " + topicName);
    }

    public static void createAcls(String topicName, String user,String groupId){

        AclBinding ac1 = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC,topicName, PatternType.LITERAL),
                new AccessControlEntry("User:"+user,"*", AclOperation.READ, AclPermissionType.ALLOW)
        );
        AclBinding ac2 = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC,topicName, PatternType.LITERAL),
                new AccessControlEntry("User:"+user,"*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
        );
        AclBinding ac3 = new AclBinding(
                new ResourcePattern(ResourceType.GROUP,groupId, PatternType.LITERAL),
                new AccessControlEntry("User:"+user,"*", AclOperation.READ, AclPermissionType.ALLOW)
        );
        AclBinding ac4 = new AclBinding(
                new ResourcePattern(ResourceType.GROUP,groupId, PatternType.LITERAL),
                new AccessControlEntry("User:"+user,"*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
        );

        adminClient.createAcls(Arrays.asList(ac1,ac2,ac3,ac4));
        System.out.println("Consumer permissions has given to user->" + user + ",  group->" + groupId + ", topic->" + topicName);
    }

    public static long getBrokerRequestPerSec(){
        try{
            JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
            JMXConnector jmxc= JMXConnectorFactory.connect(jmxServiceURL,null);
            MBeanServerConnection mbs = jmxc.getMBeanServerConnection();

            ObjectName name = new ObjectName("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce");
            Object value = mbs.getAttribute(name, "Count");
            long currentCount = (Long)value;
            long delta = currentCount - prevRequestCount;
            prevRequestCount = currentCount;
            jmxc.close();
            return delta;
        }catch (Exception e){
            System.err.println(e.getMessage());
            return 0;
        }
    }

    public static double getBrokerRequestLatencyMs(){
        try{
            JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
            JMXConnector jmxc= JMXConnectorFactory.connect(jmxServiceURL,null);
            MBeanServerConnection mbs = jmxc.getMBeanServerConnection();

            ObjectName name = new ObjectName("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce");
            Object meanLatency = mbs.getAttribute(name, "Mean");
            jmxc.close();
            return (Double)meanLatency;
        }catch (Exception e){
            System.err.println(e.getMessage());
            return 0;
        }
    }

    public static double getConsumerLag(){
        try{
            JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
            JMXConnector jmxc= JMXConnectorFactory.connect(jmxServiceURL,null);
            MBeanServerConnection mbs = jmxc.getMBeanServerConnection();

            ObjectName name = new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id=sasl-consumer");
            Object recordsLag = mbs.getAttribute(name, "records-lag-max");
            jmxc.close();
            return (Double)recordsLag;
        }catch (Exception e){
            System.err.println(e.getMessage());
            return 0;
        }
    }

    public static long getMemoryUsage() throws IOException {
        JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
        JMXConnector jmxc= JMXConnectorFactory.connect(jmxServiceURL,null);
        MBeanServerConnection mbs = jmxc.getMBeanServerConnection();

        MemoryMXBean memoryMXBean = ManagementFactory.newPlatformMXBeanProxy(mbs,ManagementFactory.MEMORY_MXBEAN_NAME,MemoryMXBean.class);

        return memoryMXBean.getHeapMemoryUsage().getUsed();
    }

    public static double getCpuUsage() throws Exception {
        JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
        JMXConnector jmxc= JMXConnectorFactory.connect(jmxServiceURL,null);
        MBeanServerConnection mbs = jmxc.getMBeanServerConnection();

        ObjectName osBeanName = new ObjectName(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);
        double cpuLoad = (Double) mbs.getAttribute(osBeanName,"ProcessCpuLoad");
        jmxc.close();
        return cpuLoad*100;
    }
}
