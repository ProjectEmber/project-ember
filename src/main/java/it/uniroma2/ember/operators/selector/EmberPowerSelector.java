package it.uniroma2.ember.operators.selector;

import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements the selector to distinguish (not)powered lamps
 */

public final class EmberPowerSelector implements OutputSelector<StreetLamp> {

    /**
     * Override select method from OutputSelector
     *
     * @param streetLamp the StreetLamp object analyzed
     * @return output as an iterable of string to split the streams
     */
    @Override
    public Iterable<String> select(StreetLamp streetLamp) {
        List<String> output = new ArrayList<String>();
        String tag = streetLamp.isPower_on() ? "on" : "off";
        output.add(tag);
        return output;
    }
}
