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

    /**
     * The class used to store the number of lamp analyzed to make the rank
     */
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

    public static class EmberLampPowerConsumptionMeans {
        private float last_hour;
        private float last_day;
        private float last_week;
    }


    /**
     * This class implements the AllWindowFunction interface in order to apply the calculation of a ranking
     *  by the value of the remaining life span of lamps
     */
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

    public static final class EmberLampPowerConsumption implements AllWindowFunction<EmberInput.StreetLamp, EmberLampPowerConsumptionMeans, TimeWindow> {

        @Override
        public void apply(TimeWindow timeWindow, Iterable<EmberInput.StreetLamp> iterable, Collector<EmberLampPowerConsumptionMeans> collector) throws Exception {

        }
    }

    /**
     *  This class implements the WindowFunction interface in order to apply the calculation of the mean value
     *  from the measures provided by ambient sensors
     */
    public static final class EmberAmbientMean implements WindowFunction<EmberInput.LumenData, Tuple2<String, Float>, String, TimeWindow> {

        /**
         * @param key the address of the ambient sensors
         * @param window used by the interface
         * @param sensors an iterable of {@link it.uniroma2.ember.EmberInput.LumenData}
         * @param collector a collector where to store the result (Tuple2<address,mean_value>)
         * @throws Exception
         */
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

    /**
     * This class implements the WindowFunction interface in order to apply the calculation of the mean value
     * from the measures provided by Traffic sensor system
     */
    public static final class EmberTrafficMean implements WindowFunction<EmberInput.TrafficData, Tuple2<String, Float>, String, TimeWindow> {

        /**
         * @param key the address of the traffic measures
         * @param window used by the interface
         * @param trafficData an iterable of {@link it.uniroma2.ember.EmberInput.TrafficData}
         * @param collector a collector where to store the result (Tuple2<address,mean_value>)
         * @throws Exception
         */
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
