package it.uniroma2.ember;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

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
     * Implements a join function to aggregate mean from traffic and lumen data streams
     */
    public static final class EmberAggregateSensors implements JoinFunction<Tuple2<String, Float>, Tuple2<String, Float>, Float> {

        @Override
        public Float join(Tuple2<String, Float> trafficData, Tuple2<String, Float> lumenData) throws Exception {
            return Math.abs(lumenData.f1 - trafficData.f1);
        }
    }
}
