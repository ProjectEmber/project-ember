package it.uniroma2.ember.elasticsearch;

import it.uniroma2.ember.utils.Alert;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Map;

/**
 *  This is an implementation of Flink Source function to retrieve
 *  failures and general alerts from the smart city grid of lights
 *  stored in Elasticsearch.
 */
public class EmberElasticsearchAlertSource implements SourceFunction<Alert> {

    private String clusterAddr = "";
    private Integer clusterPort = 9300;
    private String clusterName = "";

    /**
     * This is the default method to create a stream to process via Apache Flink
     * @param ctx, the context into which Flink and Elasticsearch are executed
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Alert> ctx) throws Exception {
        for(;;) {

            // sleeping 30 seconds before creating new alert stream
            Thread.sleep(30*1000);
        }
    }

    @Override
    public void cancel() { /* no action required */ }

    public EmberElasticsearchAlertSource(Map<String, Object> config) {
        this.clusterAddr = (String)  config.get("cluster.address");
        this.clusterPort = (Integer) config.get("cluster.port");
        this.clusterName = (String)  config.get("cluster.name");
    }
}
