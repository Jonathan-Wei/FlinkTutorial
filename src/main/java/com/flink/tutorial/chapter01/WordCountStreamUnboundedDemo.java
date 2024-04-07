package com.flink.tutorial.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception{
        //1. 创建执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建本地环境, 在IDEA中运行也可以看到webui，一般用于本地测试时使用
        // 需要引入flink-runtime-web依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //2. 读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
        //3. 处理数据
        socketDS.flatMap((String s, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).
                keyBy((Tuple2<String, Integer> t) -> t.f0)
                .sum(1).print();

        //7. 启动任务
        env.execute();
    }
}
