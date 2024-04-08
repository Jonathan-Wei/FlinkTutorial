package com.flink.tutorial.split;

import com.flink.tutorial.bean.WaterSensor;
import com.flink.tutorial.funtion.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * TODO 分流：奇数、偶数拆分不同的流
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        OutputTag<WaterSensor> s1Tag =  new OutputTag<WaterSensor>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag =  new OutputTag<WaterSensor>("s2", Types.POJO(WaterSensor.class));
        /**
         * TODO 使用侧输出 实现分流
         * 需求：WaterSensor的数据，s1,s2的数据分别分开
         */
        SingleOutputStreamOperator<WaterSensor> process = source.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    ctx.output(s1Tag, value);
                } else if ("s2".equals(value.getId())) {
                    ctx.output(s2Tag, value);
                } else {
                    out.collect(value);
                }
            }
        });

        process.print();

        process.getSideOutput(s1Tag).print();
        process.getSideOutput(s2Tag).print();


        env.execute();

    }
}
