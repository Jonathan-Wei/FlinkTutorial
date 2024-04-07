package com.flink.tutorial.transform;

import com.flink.tutorial.bean.WaterSensor;
import com.flink.tutorial.funtion.MapFuntionImpl;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // TODO map算子：对流中的每一个元素做一个映射，将其转换成另外一个元素  1->1

        // 方式一：你名内部类
//        sensorDS.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor value) throws Exception {
//                return value.getId();
//            }
//        }).print();

        // 方式二：lambda表达式
//        sensorDS.map(
//                //3.处理逻辑
//                sensor -> sensor.getId()
//        ).print();

        // 方式三：定义一个类实现MapFunction接口
//        sensorDS.map(new MyMapFunction()).print();
        sensorDS.map(new MapFuntionImpl()).print();
        //6.执行
        env.execute();
    }

    public static class MyMapFunction implements MapFunction<WaterSensor, String> {
        @Override
        public String map(WaterSensor value) throws Exception {
            return value.getId();
        }
    }
}
