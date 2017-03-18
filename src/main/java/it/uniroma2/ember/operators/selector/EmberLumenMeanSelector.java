package it.uniroma2.ember.operators.selector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Implements a simple KeySelector to divide by key (address) the lumen data - window join
 */

public final class EmberLumenMeanSelector implements KeySelector<Tuple2<String, Float>, String> {

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
