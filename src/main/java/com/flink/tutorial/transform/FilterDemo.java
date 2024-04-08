package com.flink.tutorial.transform;

import com.flink.tutorial.bean.WaterSensor;
import com.flink.tutorial.funtion.FilterFunctionImpl;
import com.flink.tutorial.funtion.MapFuntionImpl;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // TODO filter算子：过滤掉不符合条件的数据,true保留，false丢弃
//        sensorDS.filter(new FilterFunction<WaterSensor>() {
//            @Override
//            public boolean filter(WaterSensor waterSensor) throws Exception {
//                return "s1".equals(waterSensor.getId());
//            }
//        }).print();

//        sensorDS.filter(sensor -> "s1".equals(sensor.getId()) ).print();

        sensorDS.filter(new FilterFunctionImpl("s1")).print();
        //6.执行
        env.execute();
    }
}
