package it.uniroma2.ember;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;

/**
 * This is a utility class for aggregate statistics in Project Ember
 */

public class EmberSensorsAggregation {

    /**
     * Implements a simple KeySelector to divide by key (address) the traffic data - window join
     */
    public static final class EmberTrafficMeanSelector implements KeySelector<Tuple2<String, Float>, String> {

        /**
         * @param trafficData the tuple <key, traffic_intensity>
         * @return address as a key
         * @throws Exception
         */
        @Override
        public String getKey(Tuple2<String, Float> trafficData) throws Exception {
            return trafficData.f0;
        }
    }

    /**
     * Implements a simple KeySelector to divide by key (address) the lumen data - window join
     */
    public static final class EmberLumenMeanSelector implements KeySelector<Tuple2<String, Float>, String> {

        /**
         * @param lumenData the tuple <key, light_level>
         * @return address as a key
         * @throws Exception
         */
        @Override
        public String getKey(Tuple2<String, Float> lumenData) throws Exception {
            return lumenData.f0;
        }
    }

    /**
     * Implements a simple KeySelector to divide by key (address) the aggregated sensors data
     */
    public static final class EmberSensorsAddressSelector implements KeySelector<Tuple2<String,Tuple2<Float,Float>>, String> {

        /**
         * @param aggregatedSensorsData the tuple <address, Tuple2<light_value,traffic_value>>
         * @return address as a key
         * @throws Exception
         */
        @Override
        public String getKey(Tuple2<String, Tuple2<Float, Float>> aggregatedSensorsData) throws Exception {
            return aggregatedSensorsData.f0;
        }
    }



    /**
     * Implements a join function to aggregate mean from traffic and lumen data streams
     */
    public static final class EmberAggregateSensors implements JoinFunction<Tuple2<String, Float>, Tuple2<String, Float>, Tuple2<String, Tuple2<Float,Float>>> {

        @Override
        public Tuple2<String, Tuple2<Float,Float>> join(Tuple2<String, Float> trafficData, Tuple2<String, Float> lumenData) throws Exception {
            if (Objects.equals(lumenData.f0, trafficData.f0)) {
                return new Tuple2<>(lumenData.f0, new Tuple2<>(lumenData.f1, trafficData.f1));
            } else {
                return new Tuple2<>("null", new Tuple2<>(null,null));
            }
        }
    }

    /**
     * Implements a join function to set a proper level for the lamp
     * (this class actually implements the control feedback)
     */
    public static final class  EmberControlRoom implements JoinFunction<EmberInput.StreetLamp, Tuple2<String, Tuple2<Float,Float>>,EmberInput.StreetLamp> {

        /**
         * @param streetLamp a {@link it.uniroma2.ember.EmberInput.StreetLamp} instance
         * @param aggregatedSensorsData a <address, Tuple2<light_value,traffic_value>>
         * @return {@link it.uniroma2.ember.EmberInput.StreetLamp} instance with the correct level value
         * @throws Exception
         */
        @Override
        public EmberInput.StreetLamp join(EmberInput.StreetLamp streetLamp, Tuple2<String, Tuple2<Float, Float>> aggregatedSensorsData) throws Exception {
            if (!Objects.equals(aggregatedSensorsData.f0, "null")) {
                streetLamp.setLevel(aggregatedSensorsData.f1.f0 - aggregatedSensorsData.f1.f1); //TODO
                return streetLamp;
            }
            return new EmberInput.StreetLamp();
        }
    }
}
