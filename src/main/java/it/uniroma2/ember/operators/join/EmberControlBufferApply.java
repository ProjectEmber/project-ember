package it.uniroma2.ember.operators.join;


import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class EmberControlBufferApply implements WindowFunction<StreetLamp, StreetLamp, Integer, TimeWindow> {
    @Override
    public void apply(Integer s, TimeWindow window, Iterable<StreetLamp> input, Collector<StreetLamp> out) throws Exception {

        StreetLamp bufferedlamp = new StreetLamp();

        int iter = 0;
        for (StreetLamp streetlamp : input) {
            // setting streetlamp primitives
            if (iter == 0) {
                bufferedlamp.setId(streetlamp.getId());
                bufferedlamp.setAddress(streetlamp.getAddress());
                bufferedlamp.setControl_unit(streetlamp.getControl_unit());

                bufferedlamp.setModel(streetlamp.getModel());
                bufferedlamp.setLast_replacement(streetlamp.getLast_replacement());

                bufferedlamp.setConsumption(streetlamp.getConsumption());
                bufferedlamp.setLevel(0);
            }
            // buffering level control sensible to variation in the short term
            bufferedlamp.setLevel(bufferedlamp.getLevel() + streetlamp.getLevel());
            // to retrieve last valid timestamp and powered
            bufferedlamp.setSent(streetlamp.getSent());
            bufferedlamp.setPower_on(streetlamp.isPower_on());
            // incremeting iterations counter
            iter++;
        }

        // updating buffered time sensible level attribute
        bufferedlamp.setLevel(bufferedlamp.getLevel() / iter);

        out.collect(bufferedlamp);
    }
}

