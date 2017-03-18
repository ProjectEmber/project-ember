package it.uniroma2.ember.operators.selector;

import it.uniroma2.ember.utils.LumenData;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Implements a simple KeySelector to divide by key (address) the light sensors
 */

public final class EmberLumenAddressSelector implements KeySelector<LumenData, String> {

    /**
     * @param lumenData the LumenData object
     * @return LumenData.address as a string
     * @throws Exception
     */
    @Override
    public String getKey(LumenData lumenData) throws Exception {
        return lumenData.getAddress();
    }
}