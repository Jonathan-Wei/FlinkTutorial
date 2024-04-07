package com.flink.tutorial.funtion;

import com.flink.tutorial.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class MapFuntionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor waterSensor) throws Exception {
        return waterSensor.getId();
    }
}
