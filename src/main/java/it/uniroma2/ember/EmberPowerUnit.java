package it.uniroma2.ember;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;

/**
 * This is a data stream processing class to actuate the control over the
 * local power grid.
 */

public class EmberPowerUnit implements SinkFunction<EmberInput.StreetLamp>{

    @Override
    public void invoke(EmberInput.StreetLamp offStreetLamp) throws Exception {
        // actuate control on powered on task
    }
}
