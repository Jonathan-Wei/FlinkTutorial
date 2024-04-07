package com.flink.tutorial.chapter01;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SlotShadingShareDemo {
    public static void main(String[] args) throws Exception{
        //1. 创建执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建本地环境, 在IDEA中运行也可以看到webui，一般用于本地测试时使用
        // 需要引入flink-runtime-web依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 在idea运行，不指定并行度，默认就是电脑的线程数
        //env.setParallelism(1);


        //2. 读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
        socketDS.flatMap((String s, Collector<String> out) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        })
                .returns(Types.STRING).  // lambda表达式不提供足够的自动类型提取的信息。因为Java中存在泛型擦除的问题。解决方法：通过results显式的指定返回类型，来知道通过什么类型来做反序列化。
                map(word-> Tuple2.of(word,1)).slotSharingGroup("groupA").//指定当前共享组为groupA
                returns(Types.TUPLE(Types.STRING, Types.INT)).
                keyBy( value -> value.f0)
                .sum(1).print();

        //7. 启动任务
        env.execute();
    }

    /**
     * 1. Slot的特点：
     * 1）均分隔离内存，不隔离CPU
     * 2）可以共享
     *  同一个job中，不同算子的子任务可以共享同一个slot，同时在运行的
     *  前提是：属于同一个Slot共享组，默认都时“default”
     *
     *  2. Slot数量与并行度的关系
     *  1）Slot是一种静态的概念，表示最大的并行度
     *      并行度是一种动态的概念，表示实际运行占用了多少个Slot
     *  2）要求：Slot数量 >= job并行度（算子最大并行度），job才能运行
     *  注意：如果是yarn模式，动态申请
     *  --》 申请的TM数量 = job并行度/每个TM的slot数量，向上取整
     *  比如session：一开始0个 TaskManager，0个slot，等到有数据来了，才会动态申请slot
     *  --》 提交一个Job，并行度10
     *      --》 10/3 = 4，向上取整，需要4个TaskManager
     **/
}
