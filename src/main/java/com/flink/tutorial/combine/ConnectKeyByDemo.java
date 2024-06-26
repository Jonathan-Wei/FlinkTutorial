package com.flink.tutorial.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectKeyByDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<Integer,String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer,String,Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "b", 1),
                Tuple3.of(3, "c", 1)
        );


        ConnectedStreams<Tuple2<Integer, String>,Tuple3<Integer,String,Integer>> connect = source1.connect(source2);

        // 多并行度下，需要根据关联条件进行keyby，才能保证key相同的数据到一起去，才能匹配上
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectKeyByDS = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);
        /**
         *  实现互相匹配的效果：
         *  1. 两条流，不一定谁的数据先来
         *      hashmap
         *      -》 key=id,第一个字段值
         *      -》 value=List<数据>
         *  2. 每条流，有数据来，存在一个变量中
         *  3. 没条流有数据的时候，除了存变量中，去另一条流存到变量查找是否有匹配上的。
         */
        SingleOutputStreamOperator<String> process = connectKeyByDS.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
            // 定义hashmap用来存数据
            Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();


            /**
             * 处理第一条流
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement1(Tuple2<Integer, String> value, Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                // TODO 1. s1的数据来了，存到变量中
                if (!s1Cache.containsKey(id)) {
                    List<Tuple2<Integer, String>> s1Value = new ArrayList<>();
                    s1Value.add(value);
                    s1Cache.put(id, s1Value);
                } else {
                    s1Cache.get(id).add(value);
                }
                // TODO 2. 去s2的变量中查找是否有id能匹配上的，匹配上就输出，没有就不输出
                if (s2Cache.containsKey(id)) {
                    for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                        out.collect("s1: " + value + " s2: " + s2Element);
                    }
                }
            }

            /**
             * 处理第二条流
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                // TODO 1. s2的数据来了，存到变量中
                if (!s2Cache.containsKey(id)) {
                    List<Tuple3<Integer, String, Integer>> s2Value = new ArrayList<>();
                    s2Value.add(value);
                    s2Cache.put(id, s2Value);
                } else {
                    s2Cache.get(id).add(value);
                }
                // TODO 2. 去s1的变量中查找是否有id能匹配上的，匹配上就输出，没有就不输出
                if (s1Cache.containsKey(id)) {
                    for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                        out.collect("s1: " + value + " s2: " + s1Element);
                    }
                }
            }
        });
        process.print();

        env.execute();
    }
}
