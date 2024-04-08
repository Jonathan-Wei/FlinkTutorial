package com.flink.tutorial.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class KafkaSinkWithKeyDemo {
    public static void main(String[] args) throws  Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //如果是精准一次，必须开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<String> sensorDS = env.socketTextStream("localhost", 7777);

        // Kafka Sink
        /**
         * 如果要指定写入Kafka的key，可以自定义KafkaRecordSerializationSchema
         * 1. 实现一个KafkaRecordSerializationSchema接口，重写serialize方法
         * 2. 指定key，转成byte[]类型
         * 3. 指定value，转成byte[]类型
         * 4. 返回ProducerRecord<byte[], byte[]> 对象，把key和value传入
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定kafka的地址和端口
                .setBootstrapServers("localhost:9092")
                // 指定序列化器：指定Topic名称，具体的序列化方式等。
                .setRecordSerializer(
                        // 自定义Kafak序列化器，自定义key
                        new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                String[] data = element.split(",");
                                byte[]key = data[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = data[1].getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("sensor",key,value);
                            }
                        })
                // 设置Kafka的一致性规则：最多一次，至少一次，仅一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 设置精准一次，必须设置事务ID前缀
                .setTransactionalIdPrefix("flink-tutorial")
                // 如果是精准一次，必须设置事务超时时间：大于checkpoint的间隔时间,小于 max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();

        sensorDS.sinkTo(kafkaSink);

        env.execute();
    }
}
