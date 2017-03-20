package it.uniroma2.ember.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 *  This is an implementation of Flink Sink function to store in Elasticsearch
 *  data from lights and control.
 */
public class EmberElasticsearchSink implements ElasticsearchSinkFunction {

    private String index = "";
    private String type = "";
    private String clusterAddr = "";
    private Integer clusterPort = 9300;
    private String clusterName = "";

    /**
     * This method can be used to create an IndexRequest
     * @param element, the StreetLamp to store
     */
    // TODO we need to update, not to index!
    public IndexRequest createIndexRequest(StreetLamp element) {
        ObjectMapper mapper = new ObjectMapper();
        byte[] json = new byte[0];
        try {
            json = mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", this.clusterName)
                .build();

        TransportClient transportClient = null;
        try {
            transportClient = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(this.clusterAddr),this.clusterPort));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        SearchResponse response = transportClient.prepareSearch(this.index)
                .setTypes(this.type)
                .setQuery(QueryBuilders.termQuery("_id",String.valueOf(element.getId())))
                .execute()
                .actionGet();

        System.out.println(response.toString());

        return Requests.indexRequest()
                .index(this.index)
                .type(this.type)
                .id(String.valueOf(element.getId()))
                .source(json);
    }

    /**
     * @param element, a StreetLamp object
     * @param runtimeContext, the runtime context used by Flink Elasticsearch connector
     * @param indexer, the IndexRequests processor
     */
    @Override
    public void process(Object element, RuntimeContext runtimeContext, RequestIndexer indexer) {
        indexer.add(createIndexRequest((StreetLamp) element));
    }

    public EmberElasticsearchSink(String index, String type, Map<String, Object> config) {
        this.index = index;
        this.type = type;
        this.clusterAddr = (String)  config.get("cluster.address");
        this.clusterPort = (Integer) config.get("cluster.port");
        this.clusterName = (String)  config.get("cluster.name");
    }
}
