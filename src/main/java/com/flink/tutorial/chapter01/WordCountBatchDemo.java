package com.flink.tutorial.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception{
        // 1. 创建一个Flink执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据：从文件中读取数据
        String inputPath = "input/word.txt";

        DataSource<String> lineDS = env.readTextFile(inputPath);
        // 3. 切分、转换（word，1）
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });


        // 4. 按照word分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupby = wordAndOne.groupBy(0);

        // 5. 各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupby.sum(1);
        // 6. 输出结果
        sum.print();
    }
}
