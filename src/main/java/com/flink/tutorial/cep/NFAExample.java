package com.flink.tutorial.cep;

import com.flink.tutorial.bean.LoginEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class NFAExample {
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<LoginEvent, String> evnetKS = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "172.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.169.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
        ).keyBy(event -> event.userId);

        SingleOutputStreamOperator<String> warnningStream = evnetKS.flatMap(new StateMachineMapper());
        warnningStream.print();

        env.execute();

    }

    private static class StateMachineMapper extends RichFlatMapFunction<LoginEvent,String> {

        ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(LoginEvent loginEvent, Collector<String> out) throws Exception {
            State state = currentState.value();
            if(state == null){
                state = State.Initial;
            }

            // 跳转到下一个状态
            State nextState = state.transition(loginEvent.eventType);

            //判断当前状态的特殊情况，直接进行跳转
            if (nextState == State.Matched){
                // 检测到了匹配，要输出报警信息，不更新状态就是跳转回S2
                out .collect("用户" + loginEvent.userId + ",连续三次登录失败");
            }else if(nextState == State.Terminal){
                // 直接将状态更新为出事状态，重新开始检测
                currentState.update(State.Initial);
            }else{
                currentState.update(nextState);
            }
        }

    }
    public enum State {
        Terminal,// 匹配失败，终止状态
        Matched,//匹配陈工
        //S2状态，传入基于S2状态可以进行的一系列状态转移
        S2(new Transition("fail", State.Matched), new Transition("success", State.Terminal)),
        //S1状态，传入基于S1状态可以进行的一系列状态转移
        S1(new Transition("fail", State.S2), new Transition("success", State.Terminal)),
        //初始状态，传入基于初始状态可以进行的一系列状态转移
        Initial(new Transition("fail", State.S1), new Transition("success", State.Terminal));
        private Transition[] transitions;
        State(Transition ... transitions) {
            this.transitions = transitions;
        }

        public State transition(String eventType){
            for (Transition transition : transitions) {
                if (transition.getEventType().equals(eventType)){
                    return transition.getTargetState();
                }
            }
            return Initial;
        }
    }

    public static class Transition {
        private String eventType;
        private State targetState;

        public Transition(String eventType, State targetState) {
            this.eventType = eventType;
            this.targetState = targetState;
        }

        public String getEventType() {
            return eventType;
        }

        public State getTargetState() {
            return targetState;
        }
    }
}
