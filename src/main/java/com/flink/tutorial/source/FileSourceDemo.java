package com.flink.tutorial.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> source = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("input/word.txt"))
                .build();
        //TODO 从文件中读取数据：新Source架构
        env.fromSource(source, WatermarkStrategy.noWatermarks(),"fileSource")
                .print();

        env.execute();
    }

    /**
     * 新的Source写法
     *  env.fromSource(source的实现类, WatermarkStrategy,"名字")
     */
}
