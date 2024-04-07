package com.flink.tutorial.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 如果并行度是n，最大值设为a
        // 将数值均分为n份，每个并行度生成a/n个数值，
        // 比如，最大100，并行度2，每个并行度生成50个数值，其中一个并行度生成1-50，另一个生成51-100
        env.setParallelism(3);
        /**
         * 数据生成器Source：四个参数：
         *  第一个：GeneratorFunction：生成数据的函数,需要实现，重写map方法，输入类型固定为Long
         *  第二个：long类型，自动生成的数字序列（从1自增）的最大值，达到最大值后会自动停止
         *  第三个：限速策略，RateLimiterStrategy：限速策略，可以设置每秒生成多少条数据
         *  第四个：返回类型
         */
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:" + value;
                    }
                },
                1000,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );
        //TODO 从自定义数据源中读取数据：新Source架构
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(),"dataGenerator")
                .print();

        env.execute();

    }
}
