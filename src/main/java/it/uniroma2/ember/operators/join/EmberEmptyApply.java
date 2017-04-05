package it.uniroma2.ember.operators.join;

import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class EmberEmptyApply implements WindowFunction<StreetLamp, StreetLamp, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<StreetLamp> input, Collector<StreetLamp> out) throws Exception {
        for (StreetLamp streetlamp : input) {
            out.collect(streetlamp);
        }
    }
}
