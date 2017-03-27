package it.uniroma2.ember.stats;

import it.uniroma2.ember.utils.LampEMAConsumption;
import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * This class implements a WindowFunction to use Exponential Moving Average
 * on a window of values.
 */
public class EmberEMAWindowMean implements WindowFunction<StreetLamp, LampEMAConsumption, Integer, TimeWindow> {

    private static final float ALPHA = 0.85f;               // this configuration discount quickly oldest values

    /**
     * @param key the key to calculate by (id in this section)
     * @param window the time window
     * @param lamps the Iterable<{@link StreetLamp}>
     * @param collector the collector to handle the hand off between streams
     * @throws Exception
     */
    @Override
    public void apply(Integer key, TimeWindow window, Iterable<StreetLamp> lamps, Collector<LampEMAConsumption> collector) throws Exception {

        // creating lamp consumption
        LampEMAConsumption lampConsumption = new LampEMAConsumption();
        lampConsumption.setId(key);

        // iterating over lamps in window
        for (StreetLamp lamp : lamps) {
            if (lampConsumption.getConsumption() == 0) {
                // initializing average
                lampConsumption.setAddress(lamp.getAddress());
                lampConsumption.setConsumption(lamp.getConsumption());
            } else {
                // using EMA to calculate average on the selected window
                // the formula is: S_t = alpha * Y_t + (1 - alpha) * S_t-1
                lampConsumption.setConsumption(ALPHA * lamp.getConsumption() + (1 - ALPHA) * lampConsumption.getConsumption());
            }
        }

        // returning the consumption
        collector.collect(lampConsumption);
    }
}
