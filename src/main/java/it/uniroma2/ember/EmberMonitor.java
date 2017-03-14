package it.uniroma2.ember;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * This is the monitoring class, implementing the InfluxDB utilities
 */


public class EmberMonitor {

    public static final class EmberMonitorSink extends RichSinkFunction<EmberInput.StreetLamp> {

        @Override
        public void invoke(EmberInput.StreetLamp streetLamp) throws Exception {
            // TODO implement embermonitorsink invoke
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // TODO implement embermonitorsink open
        }
    }

    public static final class EmberInfluxSource implements SourceFunction<EmberInput.StreetLamp> {

        @Override
        public void run(SourceContext<EmberInput.StreetLamp> ctx) throws Exception {
            // TODO implement influx source
            ctx.collect(new EmberInput.StreetLamp());
        }

        @Override
        public void cancel() {
            this.cancel();
        }
    }

}
