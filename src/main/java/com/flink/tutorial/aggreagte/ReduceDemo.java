package com.flink.tutorial.aggreagte;

import com.flink.tutorial.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });

        /**
         * TODO reduce:两两聚合
         * 1. keyby之后才可以使用reduce算子
         * 2. 输入类型 = 输出类型，类型不能变
         * 3. 每个key的第一个数据来的时候，不会执行reduce方法，存起来，直接输出
         * 4. reduce方法中的两个参数：
         *  value1：表示上一次聚合的结果，存状态
         *  value2：表示本次要聚合的值
         */

        sensorKS.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor v1, WaterSensor v2) throws Exception {
                return new WaterSensor(v1.getId(), v1.getTs(), v1.getVc() + v2.getVc());
            }
        }).print();

//        sensorKS.reduce((value1, value2) -> {
//            System.out.println("reduce...");
//            return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
//        }).print();



        env.execute();

    }
}
