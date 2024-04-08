package com.flink.tutorial.combine;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> s2 = env.fromElements(11,22,33);
        DataStreamSource<String> s3 = env.fromElements("111", "222", "333");

        /**
         *  TODO union，合并数据流
         *  1. 数据类型必须一致
         *  2. 一次可以合并多条流
         */
//        s1.union(s2).union(s3.map(r->Integer.parseInt(r))).print();
        s1.union(s2, s3.map(r->Integer.parseInt(r))).print();

        env.execute();
    }
}
