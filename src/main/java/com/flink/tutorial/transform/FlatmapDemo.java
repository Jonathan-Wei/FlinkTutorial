package com.flink.tutorial.transform;

import com.flink.tutorial.bean.WaterSensor;
import com.flink.tutorial.funtion.MapFuntionImpl;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO 如果输入数据是sensor_1,只打印vc；如果输入数据是sensor_2,即打印ts，又打印vc
 */
public class FlatmapDemo {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // TODO flatMap算子:将每一个元素转换为0个、1个或者多个元素  一进多出（包含0出）
        /**
         *  map和flatmap的区别
         *  - map怎么控制一进一出
         *      -》 使用return
         *  - flatmap怎么控制一进多出
         *    -》 通过collector来输出，调用几次collect方法，就会输出几个元素
         */
        sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, org.apache.flink.util.Collector<String> out) throws Exception {
                // 对于s1，一进一出；
                // 对于s2，一进多出；
                // 对于s3，一进0出（类似于过滤的效果）
                if ("s1".equals(value.getId())) {
                    out.collect(value.getVc().toString());
                } else if ("s2".equals(value.getId())) {
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }
            }
        }).print();

        //6.执行
        env.execute();
    }
}
