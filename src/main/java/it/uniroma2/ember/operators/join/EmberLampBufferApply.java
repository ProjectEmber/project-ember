package it.uniroma2.ember.operators.join;

import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class EmberLampBufferApply implements WindowFunction<StreetLamp, StreetLamp, Integer, TimeWindow> {
    @Override
    public void apply(Integer s, TimeWindow window, Iterable<StreetLamp> input, Collector<StreetLamp> out) throws Exception {

        // returning to collector just buffering
        for (StreetLamp lamp : input)
            out.collect(lamp);
    }
}
