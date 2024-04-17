package com.flink.tutorial.cep;

import com.flink.tutorial.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class LoginFailDetect {
    public static void main(String[] args) throws  Exception   {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<LoginEvent> dataStream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "172.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.169.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                        return loginEvent.timestamp;
                    }
                })
        );


        // 2. 定义模式，连续三次失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first" )
                .where(new SimpleCondition<LoginEvent>(){
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                //.where()  //逻辑与的关系
                //.or() //逻辑或的关系
                // .until() //匹配到某个条件就停止, 只与oneOrMore()或者OneOrMore().optional()连用
                .next("second" )
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail" );
                    }
                })
                .next("third" )
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail" );
                    }
                });

        // 3. 应用模式
        PatternStream<LoginEvent> patternStream = CEP.pattern(dataStream.keyBy(event->event.userId), pattern);

        // 4. 将检测到的复杂时间提取出来，进行处理得到报警信息
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                // 提取复杂是贱中的三次登陆失败实践
                LoginEvent first = map.get("first").get(0);
                LoginEvent second = map.get("second").get(0);
                LoginEvent third = map.get("third").get(0);
                return first.userId+ " 连续三次登陆失败！登陆时间："+
                        first.timestamp+"-"+second.timestamp+"-"+third.timestamp;
            }
        }).print();

        env.execute();

    }
}
