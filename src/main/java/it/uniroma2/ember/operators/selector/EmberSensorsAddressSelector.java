package it.uniroma2.ember.operators.selector;


import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Implements a simple KeySelector to divide by key (address) the aggregated sensors data
 */

public final class EmberSensorsAddressSelector implements KeySelector<Tuple2<String,Tuple2<Float,Float>>, String> {

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
