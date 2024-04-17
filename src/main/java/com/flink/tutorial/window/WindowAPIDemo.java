package com.flink.tutorial.window;

import com.flink.tutorial.bean.WaterSensor;
import com.flink.tutorial.funtion.WaterSensorMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowAPIDemo {
    public static void main(String[] args) throws   Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //如果是精准一次，必须开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, Tuple> sensorKS = sensorDS.keyBy(sensorDS.getId());

        // TODO 指定窗口分配器：指定用哪一种窗口-- 时间 or 计数？滚动、滑动、会话？

        // 1.1 没有keyby的窗口：窗口内所有的数据 进入同一个子任务，并行度强行为1
//        sensorDS.windowAll();

        // 1.2 有keyby的窗口
        // 基于时间的
//        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 滚动窗口,窗口长度10s
//        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2))) // 滑动窗口,窗口长度10s,滑动步长2s
//            sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) // 会话窗口,超时间隔5s
        // 基于计数的
//        sensorKS.countWindow(5) // 滚动窗口,窗口长度=5个元素
//        sensorKS.countWindow(5,2) // 滑动窗口,窗口长度=5个元素,滑动步长=2个元素
//        sensorKS.window(GlobalWindows.create()) // 全局窗口，所有数据都进入一个窗口. 一般用于计算全局的数据指标。需要自定义一个触发器，很少用

        // TODO 2. 指定窗口函数：窗口内数据的计算逻辑
        WindowedStream<WaterSensor, Tuple, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 增量聚合：来一条算一条
//        sensorWS
//              .reduce();
//              .aggregate()
        // 全窗口函数,数据来了不计算，存起来，窗口触发的时候，计算并输出结果
//        sensorWS
//                .process()



        env.execute();
    }
}
