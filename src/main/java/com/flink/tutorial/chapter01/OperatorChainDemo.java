package com.flink.tutorial.chapter01;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class OperatorChainDemo {
    public static void main(String[] args) throws Exception{
        //1. 创建执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建本地环境, 在IDEA中运行也可以看到webui，一般用于本地测试时使用
        // 需要引入flink-runtime-web依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 在idea运行，不指定并行度，默认就是电脑的线程数

        // 算子合并条件
        // 1. One TO One的关系
        // 2. 算子的并行度相同
        // 3. 没有shuffle操作
        //env.setParallelism(1);

        // 全局禁用算子链
        //env.disableOperatorChaining();

        //2. 读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
        socketDS.flatMap((String s, Collector<String> out) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        })
//           .startNewChain()  // 开启一个新的算子链
//                .disableChaining()
                .returns(Types.STRING).  // lambda表达式不提供足够的自动类型提取的信息。因为Java中存在泛型擦除的问题。解决方法：通过results显式的指定返回类型，来知道通过什么类型来做反序列化。
                map(word-> Tuple2.of(word,1)).
                returns(Types.TUPLE(Types.STRING, Types.INT)).
                keyBy( value -> value.f0)
                .sum(1).print();

        //7. 启动任务
        env.execute();
    }

    /**
     * 1. 算子之间的传输关系
     *  一对一
     *  重分区
     * 2. 算子串在一起的条件
     * 1）一对一
     * 2）并行度相同
     * 3. 关于算子链的api
     * 1） 全局禁用算子链： env.disableOperatorChaining();
     * 2）某个算子不参与链化：算子A.disableChaining();算子A 不会与前面或者后面的算子串在一起
     * 3）从某个算子开启新链条：算子A.startNewChain(); 算子A 不与前面的算子串在一起，从A开始正常链化
     **/
}
