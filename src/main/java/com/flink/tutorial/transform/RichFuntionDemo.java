package com.flink.tutorial.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFuntionDemo {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);

        /**
         * RichXXXFunction:是Flink提供的一个富函数，可以获取运行时上下文环境
         * 1. 多了生命周期管理方法
         *  open()：初始化方法，只会在开始的时候调用一次
         *  close()：关闭方法，只会在最后的时候调用一次
         *     =》 如果flink程序异常结束，不会调用close方法
         *     =》 如果flink程序正常结束，会调用close方法
         *
         * 2. 多了一个运行时上下文
         *  可以获取一些运行时的上下文信息，比如task的名字，任务的并行度，状态
         */
        source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                RuntimeContext runtimeContext = getRuntimeContext();
                int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();
                System.out.println("subtask index: " + indexOfThisSubtask + ", task name: " + taskNameWithSubtasks + " call open()");

            }

            @Override
            public void close() throws Exception {
                super.close();
                RuntimeContext runtimeContext = getRuntimeContext();
                int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();
                System.out.println("subtask index: " + indexOfThisSubtask + ", task name: " + taskNameWithSubtasks + " call close()");

            }

            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        }).print();

        env.execute();

    }
}
