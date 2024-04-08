package com.flink.tutorial.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3);
        DataStreamSource<String> s2 = env.fromElements("111", "222", "333");

        /**
         *  TODO connect，合并数据流
         * 1. 数据类型可以不一致
         * 2. 一次只能合并两条流
         */
        ConnectedStreams<Integer, String> connect = s1.connect(s2);
        connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "s1: " + value;
            }

            @Override
            public String map2(String value) throws Exception {
                return "s2: " + value;
            }
        }).print();

        env.execute();
    }
}
