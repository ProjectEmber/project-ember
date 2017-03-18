package it.uniroma2.ember.operators.selector;

import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Implements a simple KeySelector to divide by key (address) the lamps
 */

public final class EmberLampAddressSelector implements KeySelector<StreetLamp, String> {

    /**
     * @param streetLamp the StreetLamp object
     * @return StreetLamp.address as a string
     * @throws Exception
     */
    @Override
    public String getKey(StreetLamp streetLamp) throws Exception {
        return streetLamp.getAddress();
    }
}
