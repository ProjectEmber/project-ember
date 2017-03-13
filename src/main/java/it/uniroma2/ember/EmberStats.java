package it.uniroma2.ember;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is a utility class for statistics in Project Ember
 */

public class EmberStats {

    public static final int MAX_LIFE_SPAN_DAYS = 200; // TODO by config!
    public static final int MAX_LIFE_SPAN_SIZE = 10;

    public static class EmberLampLifeSpanRank {

        private int count = 0;
        private List<EmberInput.StreetLamp> lamps = new ArrayList<>();

        public void incrementCount() {
            this.count += 1;
        }

        public void addLamp(EmberInput.StreetLamp streetLamp) {
            this.lamps.add(streetLamp);
        }

        public EmberLampLifeSpanRank() { /* */ }
    }

    public static final class EmberLampLifeSpan implements AllWindowFunction<EmberInput.StreetLamp, EmberLampLifeSpanRank, TimeWindow> {

        @Override
        public void apply(TimeWindow timeWindow, Iterable<EmberInput.StreetLamp> collection, Collector<EmberLampLifeSpanRank> collector) throws Exception {

            // using an aux class to compute the <value, lamp> array
            class Lamp implements Comparable<Lamp> {
                final int value;
                final EmberInput.StreetLamp lamp;

                Lamp(int value, EmberInput.StreetLamp lamp) {
                    this.value = value;
                    this.lamp  = lamp;
                }

                @Override
                public int compareTo(Lamp o) {
                    return ((Lamp) o).value - this.value;
                }
            }
            // creating the auxiliary array
            Lamp[] lamps = new Lamp[Iterables.size(collection)];

            // creating the life span object
            EmberLampLifeSpanRank rank = new EmberLampLifeSpanRank();

            // iterating over the collection of lamps
            int i = 0;
            for (EmberInput.StreetLamp streetLamp : collection) {
                lamps[i] = new Lamp(streetLamp.getLastReplacement(), streetLamp);
                i++;
            }

            // ordering the auxiliary array
            Arrays.sort(lamps);

            // setting rank attributes
            for (Lamp lamp : lamps) {
                rank.incrementCount();
                rank.addLamp(lamp.lamp);
                if (rank.count == MAX_LIFE_SPAN_SIZE)
                    break;
            }

            collector.collect(rank);
        }
    }

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

    public static final class EmberTrafficMean implements WindowFunction<EmberInput.TrafficData, Tuple2<String, Float>, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<EmberInput.TrafficData> trafficData,
                          Collector<Tuple2<String, Float>> collector) throws Exception {
            // iterating over ambient levels
            float ambientLevel = 0;
            int sensorsTotal = 0;
            for (EmberInput.TrafficData traffic : trafficData) {
                ambientLevel += traffic.getIntensity();
                sensorsTotal += 1;
            }

            collector.collect(new Tuple2<>(key, ambientLevel / sensorsTotal));
        }
    }

}
