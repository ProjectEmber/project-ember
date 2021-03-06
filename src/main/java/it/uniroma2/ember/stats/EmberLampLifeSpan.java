package it.uniroma2.ember.stats;

import it.uniroma2.ember.CityOfLight;
import it.uniroma2.ember.utils.EmberLampLifeSpanRank;
import it.uniroma2.ember.utils.StreetLamp;

import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * Implements an AllWindowFunction to aggregate data to perform a ranking on the increasing
 * failure rate
 */
public final class EmberLampLifeSpan implements AllWindowFunction<StreetLamp, EmberLampLifeSpanRank, TimeWindow> {

    /**
     * @param timeWindow the Window
     * @param collection the Iterable<{@link StreetLamp}>
     * @param collector  the collector to handle the hand off between streams
     * @throws Exception
     */
    @Override
    public void apply(TimeWindow timeWindow, Iterable<StreetLamp> collection, Collector<EmberLampLifeSpanRank> collector) throws Exception {

        // using an aux class to compute the <value, lamp> array
        class Lamp implements Comparable<Lamp> {
            final long value;
            final StreetLamp lamp;

            Lamp(long value, StreetLamp lamp) {
                this.value = value;
                this.lamp = lamp;
            }

            @Override
            public int compareTo(Lamp o) {
                return (int) (((Lamp) o).value - this.value);
            }
        }
        // creating the auxiliary array
        Lamp[] lamps = new Lamp[Iterables.size(collection)];

        // creating the life span object
        EmberLampLifeSpanRank rank = new EmberLampLifeSpanRank();

        // iterating over the collection of lamps
        int i = 0;
        for (StreetLamp streetLamp : collection) {
            lamps[i] = new Lamp(streetLamp.getLast_replacement(), streetLamp);
            i++;
        }

        // ordering the auxiliary array
        Arrays.sort(lamps);

        // setting rank attributes
        for (Lamp lamp : lamps) {
            rank.incrementCount();
            rank.addLamp(lamp.lamp);
            if (rank.getCount() == CityOfLight.MONITOR_MAX_LEN)
                break;
        }

        collector.collect(rank);
    }
}
