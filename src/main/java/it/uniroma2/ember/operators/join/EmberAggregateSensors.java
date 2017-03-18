package it.uniroma2.ember.operators.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;

/**
 * Implements a join function to aggregate mean from traffic and lumen data streams
 */

public final class EmberAggregateSensors implements JoinFunction<Tuple2<String, Float>, Tuple2<String, Float>, Tuple2<String, Tuple2<Float,Float>>> {

    @Override
    public Tuple2<String, Tuple2<Float,Float>> join(Tuple2<String, Float> trafficData, Tuple2<String, Float> lumenData) throws Exception {
        if (Objects.equals(lumenData.f0, trafficData.f0)) {
            return new Tuple2<>(lumenData.f0, new Tuple2<>(lumenData.f1, trafficData.f1));
        } else {
            return new Tuple2<>("null", new Tuple2<>(null,null));
        }
    }
}
