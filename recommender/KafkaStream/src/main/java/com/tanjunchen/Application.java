package com.tanjunchen;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

/**
 *
 */
public class Application {

    private static final String BROKER_URL = "192.168.17.140:9092";
    private static final String ZOOKEEPER_URL = "192.168.17.140:2181";
    private static final String KAFKA_FROM = "log";
    private static final String KAFKA_TO = "recommender";

    public static void main(String[] args) {
        String brokers = BROKER_URL;
        String zookeepers = ZOOKEEPER_URL;

        // 输入和输出的 topic
        String from = KAFKA_FROM;
        String to = KAFKA_TO;

        // 定义 kafka streaming 的配置
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        // 请设置此参数 由于 kafka 使用的版本较低, 没有设置时间戳
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        // 创建 kafka stream 配置对象
        StreamsConfig config = new StreamsConfig(props);

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
