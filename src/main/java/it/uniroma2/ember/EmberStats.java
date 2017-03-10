package it.uniroma2.ember;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * This is a utility class for statistics in Project Ember
 */

public class EmberStats {

    public static final class EmberAmbientMean implements WindowFunction<EmberInput.LumenData, Tuple2<String, Float>, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<EmberInput.LumenData> sensors,
                          Collector<Tuple2<String, Float>> collector) throws Exception {
            // iterating over ambient levels
            float ambientLevel = 0;
            int sensorsTotal = 0;
            for (EmberInput.LumenData sensor : sensors) {
                ambientLevel += sensor.getAmbient();
                sensorsTotal += 1;
            }

            collector.collect(new Tuple2<>(key, ambientLevel / sensorsTotal));
        }
    }

}
