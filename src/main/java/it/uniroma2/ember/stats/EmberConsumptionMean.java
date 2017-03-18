package it.uniroma2.ember.stats;

import it.uniroma2.ember.utils.StreetLamp;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Implements a RichFlatMap to correctly handle mean calculation using Flink managed state
 */
public class EmberConsumptionMean extends RichFlatMapFunction<StreetLamp, LampConsumption> {

    private transient ValueState<LampConsumption> consumption;

    /**
     * Override flatMap from RichFlatMap
     * This method can be used to calculate on-the-fly in a stateful flavour the consumption means
     *
     * @param streetLamp {@link StreetLamp}
     * @param collector the collector to handle the hand off
     * @throws Exception
     */
    @Override
    public void flatMap(StreetLamp streetLamp, Collector<LampConsumption> collector) throws Exception {


        LampConsumption currentConsumption = this.consumption.value();

        if (currentConsumption == null) {
            currentConsumption = new LampConsumption();
            currentConsumption.setId(streetLamp.getId());
            currentConsumption.setAddress(streetLamp.getAddress());
        }


        // update the consumption level
        currentConsumption.setConsumption(currentConsumption.getConsumption() + streetLamp.getConsumption());

        // incrementing counter - data generated every ten seconds
        currentConsumption.incrementCount();

        // computing mean on the fly
        currentConsumption.setHourMean(currentConsumption.getConsumption() /
                (currentConsumption.getCount() / 360 + 1));

        currentConsumption.setDayMean(currentConsumption.getConsumption() /
                (currentConsumption.getCount() / 8640 + 1));

        currentConsumption.setWeekMean(currentConsumption.getConsumption() /
                (currentConsumption.getCount() / 60480 + 1));

        // updating the value state
        this.consumption.update(currentConsumption);

        // collecting results
        collector.collect(currentConsumption);

    }

    /**
     * Override open from RichFunctions set
     * This method allow the retrieval of the state and set it as queryable
     */
    @Override
    @SuppressWarnings("unchecked")
    public void open(Configuration config) {
        ValueStateDescriptor descriptor =
                new ValueStateDescriptor(
                        "consumption",
                        LampConsumption.class); // default value of the state, if nothing was set
        this.consumption = getRuntimeContext().getState(descriptor);
        descriptor.setQueryable("consumption-list-api");
    }
}
