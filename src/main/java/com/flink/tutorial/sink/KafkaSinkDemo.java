package com.flink.tutorial.sink;

import com.flink.tutorial.bean.WaterSensor;
import com.flink.tutorial.funtion.WaterSensorMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaSinkDemo {
    public static void main(String[] args) throws  Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //如果是精准一次，必须开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<String> sensorDS = env.socketTextStream("localhost", 7777);

        // Kafka Sink
        /**
         * Kafka Sink :
         *  注意：如果要使用 精准一次 写入Kakfa，需要满足一下条件，缺一不可
         *  1. 必须开启checkpoint
         *  2. 必须设置事务ID前缀
         *  3. 必须设置事务超时时间：大于checkpoint的间隔时间,小于 max 15分钟
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定kafka的地址和端口
                .setBootstrapServers("localhost:9092")
                // 指定序列化器：指定Topic名称，具体的序列化方式等。
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .setTopic("sensor")
                                .build())
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
