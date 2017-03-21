package it.uniroma2.ember.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.ember.utils.Alert;
import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 *  This is an implementation of Flink Source function to retrieve
 *  failures and general alerts from the smart city grid of lights
 *  stored in Elasticsearch.
 */
public class EmberElasticsearchAlertSource implements SourceFunction<Alert> {

    private String clusterAddr = "";
    private Integer clusterPort = 9300;
    private String clusterName = "";
    private String index = "";
    private String type = "";

    /**
     * This is the default method to create a stream to process via Apache Flink
     * @param ctx, the context into which Flink and Elasticsearch are executed
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Alert> ctx) throws Exception {
        // Settings for TransportClient
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();

        for(;;) {
            TransportClient transportClient = null;
            try {
                transportClient = TransportClient.builder().settings(settings).build()
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(this.clusterAddr), this.clusterPort));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

            assert transportClient != null;
            SearchResponse response = transportClient.prepareSearch(this.index)
                    .setTypes(this.type)
                    .setQuery(matchAllQuery())
                    .setPostFilter(QueryBuilders.termQuery("power_on", false))
                    .execute()
                    .actionGet();

            // Convert results to the proper objects
            SearchHit[] results = response.getHits().getHits();
            for (SearchHit hit : results) {
                String sourceAsString = hit.getSourceAsString();
                if (sourceAsString != null) {
                    ObjectMapper mapper = new ObjectMapper();
                    StreetLamp lamp = mapper.readValue(sourceAsString, StreetLamp.class);
                    Alert alert = new Alert(lamp.getId(), lamp.getAddress(), lamp.getModel(), "error");
                    ctx.collect(alert);
                }
            }
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
        this.index = (String) config.get("index");
        this.type = (String) config.get("type");
    }
}
