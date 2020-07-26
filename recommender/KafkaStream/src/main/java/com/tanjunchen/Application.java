package com.tanjunchen;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 *
 */
public class Application {

    private static final String BROKER_URL = "192.168.17.240:9092,192.168.17.241:9092,192.168.17.242:9092";
    private static final String ZOOKEEPER_URL = "192.168.17.240:2181,192.168.17.241:2181,192.168.17.242:2181";
    private static final String KAFKA_FROM = "log";
    private static final String KAFKA_TO = "recommender";

    public static void main(String[] args) {
        String brokers = BROKER_URL;
        String zookeepers = ZOOKEEPER_URL;

        // 输入和输出的 topic
        String from = KAFKA_FROM;
        String to = KAFKA_TO;

        // 定义 kafka streaming 的配置
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        // 创建 kafka stream 配置对象
        StreamsConfig config = new StreamsConfig(settings);

        // 创建一个拓扑建构器
        TopologyBuilder builder = new TopologyBuilder();

        // 定义流处理的拓扑结构
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESSOR", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK", to, "PROCESSOR");

        KafkaStreams streams = new KafkaStreams(builder, config);

        streams.start();

        System.out.println("Kafka stream started!>>>>>>>>>>>");
    }
}
