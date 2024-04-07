package com.flink.tutorial.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception{
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. 读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");

        //3. 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        //4. 输出结果
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKeys = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });
        //5. 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordAndOneKeys.sum(1);
        //6. 输出结果
        result.print();
        //7. 启动任务
        env.execute();

    }
}
