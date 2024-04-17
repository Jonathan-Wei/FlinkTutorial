package com.flink.tutorial.window;

import com.flink.tutorial.bean.WaterSensor;
import com.flink.tutorial.funtion.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
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

public class WindowAggregateDemo {
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

        // 窗口函数：增量聚合 aggregate()
        /**
         * 1. 属于本窗口的第一条数据到来时，创建窗口，创建累计器
         * 2. 增量聚合：来一条数据，调用一次add方法
         * 3. 窗口输出时，调用getResult方法获取结果
         * 4. 输入的数据类型和输出的数据类型可以不一样，非常灵活
         */
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(
                /**
                 * 第一个类型：输入的数据类型
                 * 第二个类型：累加器的数据类型，存储中间计算结果的类型
                 * 第三个类型：输出的数据类型
                 */
                new AggregateFunction<WaterSensor, Integer, String>() {
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
        });
        aggregate.print();

        env.execute();
    }
}
