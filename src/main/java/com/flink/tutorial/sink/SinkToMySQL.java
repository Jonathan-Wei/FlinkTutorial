package com.flink.tutorial.sink;

import com.flink.tutorial.bean.WaterSensor;
import com.flink.tutorial.funtion.WaterSensorMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class SinkToMySQL {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        /**
         * TODO 写入MySQL
         * 1. 只能使用老的sink方法：addSink
         * 2. JDBC Sink 的四个参数：
         *  - 第一个参数：执行的SQL，一般就是insert into
         *  - 第二个参数：预编译的SQL，对占位符进行赋值
         *  - 第三个参数：执行选项--》攒批、重试、批次大小、批次间隔
         *  - 第四个参数：连接选项--》用户名、密码、连接地址、驱动类
         */
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.<WaterSensor>sink("insert into sensor(?,?,?,?)",
                (preparedStatement, waterSensor) -> {
                    preparedStatement.setString(1, waterSensor.getId());
                    preparedStatement.setLong(2, waterSensor.getTs());
                    preparedStatement.setInt(3, waterSensor.getVc());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test?serverTimezone=UTC&useSSL=false&characterEncoding=utf-8")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("test1234")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );

        sensorDS.addSink(jdbcSink);

        env.execute();
    }
}
