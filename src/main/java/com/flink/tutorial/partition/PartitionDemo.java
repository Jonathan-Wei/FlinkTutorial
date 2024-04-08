package com.flink.tutorial.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> source= env.socketTextStream("localhost",7777);
        // TODO shuffle 随机分区:random.nextInt(numPartitions)
//        source.shuffle().print();

        // TODO rebalance 重平衡：轮询分区,nextChannelToSend = (nextChannelToSend + 1) % 下有算子的并行度
        // 如果是数据源倾斜的场景，source读进来之后，调用rebalance，可以解决数据源倾斜的问题
//        source.rebalance().print();

        // TODO rescale缩放：实现轮训，局部组队，比rebalance更加高效
//        source.rescale().print();

        // TODO broadcast 广播：广播数据到下游算子的每个并行度
//        source.broadcast().print();

        // TODO global 全局分区：所有数据都发送到第一个分区
        source.global().print();

        //todo keyby：按指定key发送，相同key的数据发送到同一个分区
        //one to one：forword分区器

        // 总结：flink 提供了7种分区器+1种自定义

        env.execute();

    }
}
