package it.uniroma2.ember.stats;

import it.uniroma2.ember.utils.TrafficData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Implements a WindowFunction to calculate traffic mean over the specified window time
 */
public final class EmberTrafficMean implements WindowFunction<TrafficData, Tuple2<String, Float>, String, TimeWindow> {

    /**
     * @param key the key to calculate by (address in this section)
     * @param window the sliding window
     * @param trafficData the Iterable<{@link it.uniroma2.ember.EmberInput.TrafficData}>
     * @param collector the collector to handle the hand off between streams
     * @throws Exception
     */
    @Override
    public void apply(String key, TimeWindow window, Iterable<TrafficData> trafficData,
                      Collector<Tuple2<String, Float>> collector) throws Exception {
        // iterating over ambient levels
        float ambientLevel = 0;
        int sensorsTotal = 0;
        for (TrafficData traffic : trafficData) {
            ambientLevel += traffic.getIntensity();
            sensorsTotal += 1;
        }

        collector.collect(new Tuple2<>(key, ambientLevel / sensorsTotal));
    }
}
