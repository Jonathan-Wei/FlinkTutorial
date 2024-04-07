package com.flink.tutorial.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source = env
                .fromElements(1,2,33); //直接填写元素
//                .fromCollection(Arrays.asList(1, 11, 3));


        source.print();

        env.execute();
    }
}
