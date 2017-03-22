package it.uniroma2.ember.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import it.uniroma2.ember.operators.join.EmberControlRoom;
import it.uniroma2.ember.utils.Alert;
import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.*;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.search.SearchHit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 *  This is an implementation of Flink Source function to retrieve
 *  failures and general alerts from the smart city grid of lights
 *  stored in Elasticsearch.
 */
public class EmberElasticsearchAlertSource implements SourceFunction<Alert> {

    public static final long millisSleep    = 30 * 1000;
    public static final long failureSeconds = 30 * 3;
    public static final long replacementExp = 17000000;

    private String index = "";
    private String typeLamp = "";
    private String typeControl = "";
    private String clusterAddr = "";
    private Integer clusterPort = 9300;
    private String clusterName = "";

    public List<Alert> alertQuery() throws Exception {

        // alert list
        List<Alert> alertList = new ArrayList<>();

        // settings for transport client
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();

        // creating transport client
        TransportClient transportClient = null;
        try {
            transportClient = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(this.clusterAddr), this.clusterPort));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        assert transportClient != null;

        try {
            transportClient.admin().indices().prepareCreate("ember").get();
        } catch (IndexAlreadyExistsException e) {
        }

        // we assume that the level and power_on is always reached, if it is not
        // the local control unit will stop notify the system generating a 'timestamp' error
        //
        // for all elements in 'lamp' type
        BoolQueryBuilder failuresQuery = boolQuery()
                // -> check if timestamp is too far from current (not responding) (~90 seconds)
                .should(rangeQuery("sent").lt(Instant.now().getEpochSecond() - failureSeconds))
                // -> check if the lamp is near expiration (~200 days)
                .should(rangeQuery("last_replacement").lt(Instant.now().getEpochSecond() - replacementExp))
                // -> check if level is out of security levels
                .should(
                        boolQuery()
                            .must(termQuery("power_on", true))
                            .should(rangeQuery("level").lt(EmberControlRoom.LAMP_SECURITY_LEVEL))
                            .should(rangeQuery("level").gt(EmberControlRoom.TRAFFIC_MAJOR_LEVEL)));

        // retrieving failures
        SearchResponse response = transportClient.prepareSearch(this.index)
                .setTypes(this.typeLamp)
                .setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(failuresQuery)
                .execute()
                .actionGet();

        // convert results to the proper objects
        SearchHit[] results = response.getHits().getHits();
        for (SearchHit hit : results) {
            String sourceAsString = hit.getSourceAsString();
            if (sourceAsString != null) {
                // creating a new streetlamp and alert objects
                Gson gson = new Gson();
                StreetLamp lamp = gson.fromJson(sourceAsString, StreetLamp.class);
                Alert alert = new Alert(lamp.getId(), lamp.getAddress(), lamp.getModel(), "");

                // making comparison to decide which error occurred
                if (lamp.getSent() <= Instant.now().getEpochSecond() - failureSeconds)
                    alert.setMessage(alert.getMessage() + Alert.ERROR_SENT);
                if (lamp.getLast_replacement() <= Instant.now().getEpochSecond() - replacementExp)
                    alert.setMessage(alert.getMessage() + Alert.ERROR_EXPIRE);
                if (lamp.getLevel() < EmberControlRoom.LAMP_SECURITY_LEVEL || lamp.getLevel() > EmberControlRoom.TRAFFIC_MAJOR_LEVEL)
                    alert.setMessage(alert.getMessage() + Alert.ERROR_LEVEL);

                // appending alert to list
                alertList.add(alert);
            }
        }

        // returning alert list
        return alertList;
    }

    /**
     * This is the default method to create a stream to process via Apache Flink
     * @param ctx, the context into which Flink and Elasticsearch are executed
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Alert> ctx) throws Exception {

        for(;;) {
            // collecting alerts raised
            for (Alert al : alertQuery())
                ctx.collect(al);
            // sleeping 30 seconds before creating new alert stream
            Thread.sleep(millisSleep);
        }
    }

    @Override
    public void cancel() { /* no action required */ }

    public EmberElasticsearchAlertSource(String index, String typeLamp, Map<String, Object> config) {
        this.index       = index;
        this.typeLamp    = typeLamp;
        this.clusterAddr = (String)  config.get("cluster.address");
        this.clusterPort = (Integer) config.get("cluster.port");
        this.clusterName = (String)  config.get("cluster.name");
    }
}
