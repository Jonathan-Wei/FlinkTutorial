package com.flink.tutorial.window;

import com.flink.tutorial.bean.WaterSensor;
import com.flink.tutorial.funtion.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowReduceDemo {
    public static void main(String[] args) throws   Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //如果是精准一次，必须开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, Tuple> sensorKS = sensorDS.keyBy(sensorDS.getId());

        // TODO 指定窗口分配器
        WindowedStream<WaterSensor, Tuple, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // 窗口函数：增量聚合 reduce()
        /**
         * 窗口的reduce：
         * 1. 相同key的第一条数据不会进入reduce方法
         * 2. 增量聚合：来一条算一条，但是不会输出
         * 3. 在窗口触发的时候，才会输出窗口的最终计算结果
         */
        SingleOutputStreamOperator<WaterSensor> reduce = sensorWS.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor s1, WaterSensor s2) throws Exception {
                System.out.println("调用reduce方法，value1=" + s1 + ",value2=" + s2);
                return new WaterSensor(s1.getId(), s2.getTs(), s1.getVc() + s2.getVc());
            }
        });

        reduce.print();


        env.execute();
    }
}
