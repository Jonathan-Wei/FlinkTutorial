package com.flink.tutorial.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSourceDemo {
    public static void main(String[] args)  throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 从kafka读取数据：新Source架构
        KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092") //指定kafka的地址和端口
                .setTopics("sensor")//指定消费的topic
                .setGroupId("flink")//指定消费者组id
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定反序列化器
                .setStartingOffsets(OffsetsInitializer.earliest())//指定消费策略
                .build();

    }

    /**
     * Kafka的消费者参数：
     *    auto.reset.offset:
     *      earliest: 如果有offset，从offset开始消费，如果没有offset，从最早消费
     *      latest： 如果有offset，从offset开始消费，如果没有offset，从最新消费
     *
     *  Flink的kafkaSource，offset的消费策略：OffsetsInitializer
     *      earliest：从最早的offset开始消费
     *      lastest：从最新的offset开始消费
     *
     */
}
