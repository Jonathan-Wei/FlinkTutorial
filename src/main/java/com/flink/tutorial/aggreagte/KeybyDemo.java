package com.flink.tutorial.aggreagte;

import com.flink.tutorial.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeybyDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // 按照id分组
        /**
         * TODO keyby：按照id分组
         * 要点：
         * 1. 返回的是一个KeyedStream，键控流
         * 2. 说明keyby不是转换算子，只是对数据进行重分区，不能设置并行度
         * 3. keyby 分组与分区的关系
         * 比如：学生、教室，学生分了小组 1、2、3组
         *  1）keyby 是对数据分组，保证相同key的数据在同一个分区
         *  2）分区：一个子任务，可以理解为一个分区
         *
         */
        sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        }).print();

        sensorDS.keyBy(WaterSensor::getId).print();

        env.execute();

    }
}
