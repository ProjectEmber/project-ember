package it.uniroma2.ember.operators.selector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Implements a simple KeySelector to divide by key (address) the traffic data - window join
 */

public final class EmberTrafficMeanSelector implements KeySelector<Tuple2<String, Float>, String> {

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
