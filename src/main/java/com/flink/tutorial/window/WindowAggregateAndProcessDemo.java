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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowAggregateAndProcessDemo {
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

        /**
         * 窗口函数：增量聚合AggregateFunction和全窗口函数ProcessWindowFunction
         * 1. 增量聚合函数处理数据：来一条处理一条
         * 2. 窗口触发时，增量聚合的结果（只有一条）传递给全窗口函数
         * 3. 经过全窗口函数处理包装后，输出
         *
         * 结合两者的优点：
         * 1. 增量聚合函数处理数据：来一条处理一条，存储中间的计算结果，占用的空间少
         * 2. 全窗口函数处理数据：可以通过上下文实现灵活的功能
         */

        // sensorWS.reduce() //也可以传递两个参数
        SingleOutputStreamOperator<String> result = sensorWS.aggregate(
            new MyAgg(),new MyProcess()
        );
        result.print();

        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor,Integer,String>{
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器");
                return 0;
            }

            @Override
            public Integer add(WaterSensor value, Integer accumulator) {
                System.out.println("调用add方法，value="+value);
                return accumulator + value.getVc();
            }

            @Override
            public String getResult(Integer accumulator) {
                System.out.println("获取结果");
                return accumulator.toString();
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                // 只有会话窗口才会用到
                return a + b;
            }
    }
    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            System.out.println("window触发了，窗口的开始时间是：" + context.window().getStart() + "，窗口的结束时间是：" + context.window().getEnd());
            for (String waterSensor : elements) {
                out.collect(waterSensor.toString());
            }
        }
    }
}
