package com.flink.tutorial.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CustomeSink {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //如果是精准一次，必须开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<String> sensorDS = env.socketTextStream("localhost", 7777);

        sensorDS.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<String> {
        /**
         * Sink 核心逻辑，写出的逻辑就写在invoke方法中
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(String value, Context context) throws Exception {
            // 写入逻辑：例如StarRocks
            // 这个方法是 来一条数据，写一条数据,所以不要在这里创建连接对象

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在这里创建连接
        }

        @Override
        public void close() throws Exception {
            super.close();
            // 清理、销毁连接
        }
    }
}
