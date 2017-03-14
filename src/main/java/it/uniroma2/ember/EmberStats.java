package it.uniroma2.ember;

/**
 * This is a utility class for statistics in Project Ember
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.types.ListValue;
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
     * Implements a WindowFunction to calculate ambient light mean over the specified window time
     */
    public static final class EmberAmbientMean implements WindowFunction<EmberInput.LumenData, Tuple2<String, Float>, String, TimeWindow> {

        /**
         * @param key the key to calculate by (address in this section)
         * @param window the sliding window
         * @param sensors the Iterable<{@link it.uniroma2.ember.EmberInput.LumenData}>
         * @param collector the collector to handle the hand off between streams
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
     * Implements a WindowFunction to calculate traffic mean over the specified window time
     */
    public static final class EmberTrafficMean implements WindowFunction<EmberInput.TrafficData, Tuple2<String, Float>, String, TimeWindow> {

        /**
         * @param key the key to calculate by (address in this section)
         * @param window the sliding window
         * @param trafficData the Iterable<{@link it.uniroma2.ember.EmberInput.TrafficData}>
         * @param collector the collector to handle the hand off between streams
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

    /**
     * Implements a simple object to represent a ranking of the lamp to be replaced
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

    /**
     * Implements an AllWindowFunction to aggregate data to perform a ranking on the increasing
     * failure rate
     */
    public static final class EmberLampLifeSpan implements AllWindowFunction<EmberInput.StreetLamp, EmberLampLifeSpanRank, TimeWindow> {

        /**
         * @param timeWindow the Window
         * @param collection the Iterable<{@link it.uniroma2.ember.EmberInput.StreetLamp}>
         * @param collector  the collector to handle the hand off between streams
         * @throws Exception
         */
        @Override
        public void apply(TimeWindow timeWindow, Iterable<EmberInput.StreetLamp> collection, Collector<EmberLampLifeSpanRank> collector) throws Exception {

            // using an aux class to compute the <value, lamp> array
            class Lamp implements Comparable<Lamp> {
                final int value;
                final EmberInput.StreetLamp lamp;

                Lamp(int value, EmberInput.StreetLamp lamp) {
                    this.value = value;
                    this.lamp = lamp;
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

    /**
     * Implements a simple FlatMapFunction to parse StreetLamp object into a JSON string
     */
    public static final class EmberSerializeRank implements FlatMapFunction<EmberStats.EmberLampLifeSpanRank, String> {

        /**
         * Override flatMap method from FlatMapFunction
         *
         * @param lifeSpanRank, the {@link it.uniroma2.ember.EmberStats.EmberLampLifeSpanRank} object to be parsed
         * @param collector the Collector<String> to handle the control stream handoff
         */
        @Override
        public void flatMap(EmberStats.EmberLampLifeSpanRank lifeSpanRank, Collector<String> collector) throws Exception {
            collector.collect(new ObjectMapper().writeValueAsString(lifeSpanRank));
        }
    }

    /**
     * Implements a simple object to represent in a state list the lamp power consumption
     */
    public static class LampConsumption {

        private int id          = 0;
        private int count       = 0;
        private float hourMean  = 0;
        private float dayMean   = 0;
        private float weekMean  = 0;
        private String address;


        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getCount() {
            return count;
        }

        public void incrementCount() {
            count += 1;
        }

        public float getHourMean() {
            return hourMean;
        }

        public void setHourMean(float hourMean) {
            this.hourMean = hourMean;
        }

        public float getDayMean() {
            return dayMean;
        }

        public void setDayMean(float dayMean) {
            this.dayMean = dayMean;
        }

        public float getWeekMean() {
            return weekMean;
        }

        public void setWeekMean(float weekMean) {
            this.weekMean = weekMean;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public LampConsumption() { /* */ }
    }

    /**
     * Implements a RichFlatMap to correctly handle mean calculation using Flink managed state
     */
    public static class EmberConsumptionMean extends RichFlatMapFunction<EmberInput.StreetLamp, LampConsumption> {

        private transient ListState<LampConsumption> consumptionList;
        private static LampConsumption current = new LampConsumption();

        /**
         * Override flatMap from RichFlatMap
         * This method can be used to calculate on-the-fly in a stateful flavour the consumption means
         *
         * @param streetLamp {@link it.uniroma2.ember.EmberInput.StreetLamp}
         * @param collector the collector to handle the hand off
         * @throws Exception
         */
        @Override
        public void flatMap(EmberInput.StreetLamp streetLamp, Collector<LampConsumption> collector) throws Exception {

            // access the state value ...
            boolean newC = false;
            LampConsumption currentConsumption = null;
            for (LampConsumption l : this.consumptionList.get()) {
                if (l.getId() == streetLamp.getId()) {
                    currentConsumption = l;
                    break;
                }
            }
            // ... or creating a new state value
            if (currentConsumption == null) {
                newC = true;
                currentConsumption = new LampConsumption();
                currentConsumption.setId(streetLamp.getId());
                currentConsumption.setAddress(streetLamp.getAddress());
            }


            // update the mean every hour
            currentConsumption.setHourMean(currentConsumption.getHourMean() + streetLamp.getConsumption());
            currentConsumption.setDayMean(currentConsumption.getDayMean() + streetLamp.getConsumption());
            currentConsumption.setWeekMean(currentConsumption.getWeekMean() + streetLamp.getConsumption());


            // incrementing counter - every ten seconds if it is right
            currentConsumption.incrementCount();

            // update the state
            // TODO check if it updates correctly! even if it is not a newConsumption
            if (newC)
                this.consumptionList.add(currentConsumption);

            // computing mean
            // TODO calculate proper mean
            current = currentConsumption;

            // collecting results
            collector.collect(current);

        }

        /**
         * Override open from RichFunctions set
         * This method allow the retrieval of the state and set it as queryable
         */
        @Override
        @SuppressWarnings("unchecked")
        public void open(Configuration config) {
            ListStateDescriptor descriptor =
                    new ListStateDescriptor(
                            "consumption",
                            LampConsumption.class); // default value of the state, if nothing was set
            this.consumptionList = getRuntimeContext().getListState(descriptor);
            descriptor.setQueryable("consumption-list-api");
        }
    }

    /**
     * Implements a simple FlatMapFunction to parse LampConsumption object into a JSON string
     */
    public static final class EmberSerializeConsumption implements FlatMapFunction<EmberStats.LampConsumption, String> {

        /**
         * Override flatMap method from FlatMapFunction
         *
         * @param lampConsumption, the {@link it.uniroma2.ember.EmberStats.LampConsumption} object to be parsed
         * @param collector the Collector<String> to handle the control stream handoff
         */
        @Override
        public void flatMap(EmberStats.LampConsumption lampConsumption, Collector<String> collector) throws Exception {
            collector.collect(new ObjectMapper().writeValueAsString(lampConsumption));
        }
    }
}
