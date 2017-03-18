package it.uniroma2.ember.operators.selector;

import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Implements a simple KeySelector to divide by key (id) the lamps
 */

public final class EmberLampIdSelector implements KeySelector<StreetLamp, Integer> {

    /**
     * @param streetLamp the StreetLamp object
     * @return StreetLamp.id as an Integer
     * @throws Exception
     */
    @Override
    public Integer getKey(StreetLamp streetLamp) throws Exception {
        return streetLamp.getId();
    }
}
