package it.uniroma2.ember.stats;

import it.uniroma2.ember.utils.LampEMAConsumptionStreet;
import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * This class implements a WindowFunction to use Exponential Moving Average
 * on a window of values.
 */
public class EmberEMAWindowMeanAddress implements WindowFunction<StreetLamp, LampEMAConsumptionStreet, String, TimeWindow> {

    private static final float ALPHA = 0.85f;               // this configuration discount quickly oldest values


    /**
     * @param key the key to calculate by (address or id in this section)
     * @param window the time window
     * @param lamps the Iterable<{@link StreetLamp}>
     * @param collector the collector to handle the hand off between streams
     * @throws Exception
     */
    @Override
    public void apply(String key, TimeWindow window, Iterable<StreetLamp> lamps, Collector<LampEMAConsumptionStreet> collector) throws Exception {

        // creating lamp consumption
        LampEMAConsumptionStreet streetConsumption = new LampEMAConsumptionStreet();
        streetConsumption.setAddress(key);
        streetConsumption.setComputed(Instant.now().getEpochSecond());

        // iterating over lamps in window
        for (Object lamp : lamps) {
            if (streetConsumption.getConsumption() == 0) {
                // initializing average
                streetConsumption.setConsumption(((StreetLamp) lamp).getConsumption());
            } else {
                // using EMA to calculate average on the selected window
                // the formula is: S_t = alpha * Y_t + (1 - alpha) * S_t-1
                streetConsumption.setConsumption(ALPHA * ((StreetLamp) lamp).getConsumption() + (1 - ALPHA) * streetConsumption.getConsumption());
            }
        }

        // returning the consumption
        collector.collect(streetConsumption);
    }
}
