package com.flink.tutorial.window;

import com.flink.tutorial.bean.WaterSensor;
import com.flink.tutorial.funtion.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WindowProcessDemo {
    public static void main(String[] args) throws   Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //如果是精准一次，必须开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor,String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // TODO 指定窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // 窗口函数：全窗口函数 process()

        // 老写法
//        sensorWS
//                .apply(
//                new WindowFunction<WaterSensor, String, String, TimeWindow>() {
//                    /**
//                     *
//                     * @param s 分组的key
//                     * @param window 窗口
//                     * @param input 窗口内的数据
//                     * @param out 输出
//                     * @throws Exception
//                     */
//                    @Override
//                    public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, org.apache.flink.util.Collector<String> out) throws Exception {
//                        System.out.println("window触发了，窗口的开始时间是：" + window.getStart() + "，窗口的结束时间是：" + window.getEnd());
//                        for (WaterSensor waterSensor : input) {
//                            out.collect(waterSensor.toString());
//                        }
//                    }
//                }
//        );

        // 新写法
        sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            /**
             *
             * @param s 分组的key
             * @param context 上下文
             * @param elements 窗口内的数据
             * @param out 输出
             * @throws Exception
             */
            @Override
            public void process(String s, Context context, Iterable<WaterSensor> elements, org.apache.flink.util.Collector<String> out) throws Exception {
                // 通过上下文可以拿到窗口对象，还有其他信息，比如侧输出流，等等
                System.out.println("window触发了，窗口的开始时间是：" + context.window().getStart() + "，窗口的结束时间是：" + context.window().getEnd());
                long startTs = context.window().getStart();
                long endTs = context.window().getEnd();
                String windowStart = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(startTs));
                String windowEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(endTs));
                long count = elements.spliterator().estimateSize();
                out.collect("key:" + s + "的窗口["+windowStart+","+windowEnd+"),包含 " + count + "条数据  =="+elements.toString());
            }
        }).print();


        env.execute();
    }
}
