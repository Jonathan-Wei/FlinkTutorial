package com.flink.tutorial.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionCustomeDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> source= env.socketTextStream("localhost",7777);

        source.partitionCustom(new MyPartitioner(),r-> r).print();

        env.execute();
    }
}
