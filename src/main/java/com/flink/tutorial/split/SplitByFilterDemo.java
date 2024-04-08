package com.flink.tutorial.split;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO 分流：奇数、偶数拆分不同的流
 */
public class SplitByFilterDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> source= env.socketTextStream("localhost",7777);

        source.filter(value->Integer.parseInt(value)%2==0).print("偶数流");
        source.filter(value->Integer.parseInt(value)%2==1).print("奇数流");

        env.execute();

    }
}
