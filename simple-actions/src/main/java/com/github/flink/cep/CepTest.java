package com.github.flink.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.RichPatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @Author: zlzhang0122
 * @Date: 2020/12/30 4:42 下午
 */
public class CepTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // DataStream : source
        DataStream<TemperatureEvent> input = env.fromElements(new TemperatureEvent(1,"Device01", 22.0),
                new TemperatureEvent(1,"Device01", 27.1), new TemperatureEvent(2,"Device01", 28.1),
                new TemperatureEvent(1,"Device01", 22.2), new TemperatureEvent(3,"Device01", 22.1),
                new TemperatureEvent(1,"Device02", 22.3), new TemperatureEvent(4,"Device02", 22.1),
                new TemperatureEvent(1,"Device02", 22.4), new TemperatureEvent(5,"Device02", 22.7),
                new TemperatureEvent(1,"Device02", 27.0), new TemperatureEvent(6,"Device02", 30.0));

        Pattern<TemperatureEvent, ?> warningPattern = Pattern.<TemperatureEvent>begin("start")
                .subtype(TemperatureEvent.class)
                .where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent subEvent) {
                        if (subEvent.getTemperature() >= 26.0)

                        { return true; }
                        return false;
                    }
                }).where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent subEvent) {
                        if (subEvent.getMachineName().equals("Device02")) { return true; }
                        return false;
                    }
                }).within(Time.seconds(10));

        DataStream<String> patternStream = CEP.pattern(input, warningPattern)
                .select(
                        new RichPatternSelectFunction<TemperatureEvent, String>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                System.out.println(getRuntimeContext().getUserCodeClassLoader());
                            }

                            @Override
                            public String select(Map<String, List<TemperatureEvent>> event) throws Exception {
                                return new String("Temperature Rise Detected: " + event.get("start") + " on machine name: " + event.get("start"));
                            }
                        });

        patternStream.print();

        env.execute("CEP on Temperature Sensor");
    }
}
