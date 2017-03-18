package it.uniroma2.ember.operators.selector;

import it.uniroma2.ember.utils.TrafficData;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Implements a simple KeySelector to divide by key (address) the traffic data
 */

public final class EmberTrafficAddressSelector implements KeySelector<TrafficData, String> {

    /**
     * @param trafficData the TrafficData object
     * @return TrafficData.address as a string
     * @throws Exception
     */
    @Override
    public String getKey(TrafficData trafficData) throws Exception {
        return trafficData.getAddress();
    }
}
