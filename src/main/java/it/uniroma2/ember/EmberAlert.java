package it.uniroma2.ember;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * This is the monitoring class, implementing the InfluxDB utilities
 */


public class EmberAlert {

    public static final class EmberLampSink extends RichSinkFunction<EmberInput.StreetLamp> {

        @Override
        public void invoke(EmberInput.StreetLamp streetLamp) throws Exception {
            // TODO implement alert sink invoke
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // TODO implement alert sink open
        }
    }

    public static final class EmberControlSink extends RichSinkFunction<EmberInput.StreetLamp> {

        @Override
        public void invoke(EmberInput.StreetLamp streetLamp) throws Exception {
            // TODO implement control invoke
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // TODO implement control open
        }
    }


    public static class Alert {
        private int id;
        private String address;
        private String model;
        private String message;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public Alert() { /* */ }
    }

    public static final class EmberInfluxSource implements SourceFunction<Alert> {

        @Override
        public void run(SourceContext<Alert> ctx) throws Exception {
            // TODO implement influx source
            ctx.collect(new Alert());
        }

        @Override
        public void cancel() {
            this.cancel();
        }
    }

    public static String serializeAlert(Alert alert) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(alert);

    }

    public static final class EmberSerializeAlert implements FlatMapFunction<Alert, String> {

        @Override
        public void flatMap(Alert alert, Collector<String> collector) throws Exception {
            collector.collect(serializeAlert(alert));
        }
    }

}
