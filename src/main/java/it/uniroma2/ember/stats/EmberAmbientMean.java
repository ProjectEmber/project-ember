package it.uniroma2.ember.stats;

/**
 * Created by federico on 18/03/17.
 */

import it.uniroma2.ember.EmberInput;
import it.uniroma2.ember.utils.LumenData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Implements a WindowFunction to calculate ambient light mean over the specified window time
 */
public final class EmberAmbientMean implements WindowFunction<LumenData, Tuple2<String, Float>, String, TimeWindow> {

    /**
     * @param key the key to calculate by (address in this section)
     * @param window the sliding window
     * @param sensors the Iterable<{@link it.uniroma2.ember.EmberInput.LumenData}>
     * @param collector the collector to handle the hand off between streams
     * @throws Exception
     */
    @Override
    public void apply(String key, TimeWindow window, Iterable<LumenData> sensors,
                      Collector<Tuple2<String, Float>> collector) throws Exception {
        // iterating over ambient levels
        float ambientLevel = 0;
        int sensorsTotal = 0;
        for (LumenData sensor : sensors) {
            ambientLevel += sensor.getAmbient();
            sensorsTotal += 1;
        }

        collector.collect(new Tuple2<>(key, ambientLevel / sensorsTotal));
    }
}