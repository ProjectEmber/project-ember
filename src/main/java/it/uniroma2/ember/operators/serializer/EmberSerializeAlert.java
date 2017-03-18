package it.uniroma2.ember.operators.serializer;

import it.uniroma2.ember.utils.Alert;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;


public final class EmberSerializeAlert implements FlatMapFunction<Alert, String> {

    @Override
    public void flatMap(Alert alert, Collector<String> collector) throws Exception {
        collector.collect(Alert.serializeAlert(alert));
    }
}