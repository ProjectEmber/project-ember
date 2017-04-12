package it.uniroma2.ember.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import it.uniroma2.ember.CityOfLight;
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
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 *  This is an implementation of Flink Source function to retrieve
 *  failures and general alerts from the smart city grid of lights
 *  stored in Elasticsearch.
 */
public class EmberElasticsearchAlertSource implements SourceFunction<Alert> {

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
                // -> check if timestamp is too far from current (not responding) (~90 seconds by default)
                .should(rangeQuery("sent").lt(Instant.now().getEpochSecond() - CityOfLight.TO_FAILURE_SECONDS))
                // -> check if the lamp is near expiration (~200 days)
                .should(rangeQuery("last_replacement").lt(Instant.now().getEpochSecond() - TimeUnit.DAYS.toSeconds(CityOfLight.MAX_LIFE_SPAN_DAYS)))
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

                Alert alert = new Alert();
                alert.setId(lamp.getId());
                alert.setAddress(lamp.getAddress());
                alert.setModel(lamp.getModel());
                alert.setRaised(Instant.now().getEpochSecond());

                // making comparison to decide which error occurred
                if (lamp.getSent() <= Instant.now().getEpochSecond() - CityOfLight.TO_FAILURE_SECONDS) {
                    alert.setMessage(alert.getMessage() + Alert.ERROR_SENT);
                    alert.setElectrical_failure(true);
                }
                if (lamp.getLast_replacement() <= Instant.now().getEpochSecond() - TimeUnit.DAYS.toSeconds(CityOfLight.MAX_LIFE_SPAN_DAYS)) {
                    alert.setMessage(alert.getMessage() + Alert.ERROR_EXPIRE);
                    alert.setExpiration(true);
                }
                if (lamp.getLevel() < EmberControlRoom.LAMP_SECURITY_LEVEL || lamp.getLevel() > EmberControlRoom.TRAFFIC_MAJOR_LEVEL) {
                    alert.setMessage(alert.getMessage() + Alert.ERROR_LEVEL);
                    alert.setLumen_level_control(true);
                }

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
            Thread.sleep(CityOfLight.ALERT_SOURCE_PERIOD_SECONDS);
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
