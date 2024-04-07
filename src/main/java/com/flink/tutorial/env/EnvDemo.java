package com.flink.tutorial.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class EnvDemo {
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT,"8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(conf); //conf 对象可以修改flink的配置
//                .getExecutionEnvironment();  // 自动识别是远程集群，还是idea本地环境
//                .createLocalEnvironment()
//                .createRemoteEnvironment("flink001",8081,"/xxx")


        //流批一体：代码api是同一套，可以指定为批，也可以指定为流
        //默认：Streaming
        //一般不在代码中指定，而是在提交任务时参数指定： -Dexecution.runtime-mode=BATCH
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING); //流处理
//                .setRuntimeMode(RuntimeExecutionMode.BATCH);//批处理

        env
                .readTextFile("input/word.txt")
//                .socketTextStream("localhost", 7777)
                .flatMap((String s, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = s.split(" ");
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT)).
                keyBy((Tuple2<String, Integer> t) -> t.f0)
                .sum(1).print();

        //7. 启动任务
        /**
         * TODO 关于execute的总结
         * 1. 默认 env.execute() 触发一个Flink Job的执行
         *  -- 一个main方法可以调用多个execute()，但是没有意义，指定到第一个就会阻塞
         *  2. env.executeAsync() 异步执行
         *      -> 一个main方法里executeAsync()个数=生成的flink job个数
         *  3. 思考：
         *      yarn-application 集群，提交一次，集群里面会有几个flink job
         *      =》 取决于调用了几个executeAsync()
         *      =》 对应application集群里，会有n个job
         *      =》对应JobManager里，会有n个JobMaster
         */
        env.execute();


        //异步执行
        //env.executeAsync();

    }
}
