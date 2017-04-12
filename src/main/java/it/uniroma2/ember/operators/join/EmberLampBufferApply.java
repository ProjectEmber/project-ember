package it.uniroma2.ember.operators.join;

import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class EmberLampBufferApply implements WindowFunction<StreetLamp, StreetLamp, Integer, TimeWindow> {
    @Override
    public void apply(Integer s, TimeWindow window, Iterable<StreetLamp> input, Collector<StreetLamp> out) throws Exception {
//        StreetLamp bufferedlamp = new StreetLamp();
//
//        int iter = 0;
//        for (StreetLamp streetlamp : input) {
//            // setting streetlamp primitives
//            if (iter == 0) {
//                bufferedlamp.setId(streetlamp.getId());
//                bufferedlamp.setAddress(streetlamp.getAddress());
//                bufferedlamp.setControl_unit(streetlamp.getControl_unit());
//
//                bufferedlamp.setModel(streetlamp.getModel());
//                bufferedlamp.setLast_replacement(streetlamp.getLast_replacement());
//
//                bufferedlamp.setConsumption(0);
//                bufferedlamp.setLevel(0);
//            }
//            // buffering data sensible to variation in the short term
//            bufferedlamp.setConsumption(bufferedlamp.getConsumption() + streetlamp.getConsumption());
//            bufferedlamp.setLevel(bufferedlamp.getLevel() + streetlamp.getLevel());
//            // to retrieve last valid timestamp
//            bufferedlamp.setSent(streetlamp.getSent());
//            // incremeting iterations counter
//            iter++;
//        }
//
//        // updating buffered time sensible attributes
//        bufferedlamp.setConsumption(bufferedlamp.getConsumption() / iter);
//        bufferedlamp.setLevel(bufferedlamp.getLevel() / iter);

        // returning to collector
        for (StreetLamp lamp : input)
            out.collect(lamp);
    }
}
