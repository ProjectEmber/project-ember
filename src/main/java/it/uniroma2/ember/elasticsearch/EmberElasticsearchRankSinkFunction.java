package it.uniroma2.ember.elasticsearch;

import it.uniroma2.ember.utils.EmberLampLifeSpanRank;
import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;


public class EmberElasticsearchRankSinkFunction implements ElasticsearchSinkFunction<EmberLampLifeSpanRank> {

    private String index = "";
    private String type = "";

    /**
     * This method can be used to create an IndexRequest
     *
     * @param lamp, the StreetLamp to store
     */
    private IndexRequest createIndexRequest(StreetLamp lamp, Integer position) throws Exception {

        byte[] convertedElem = new ObjectMapper().writeValueAsBytes(lamp);

        // creating update request using position to have an updated rank
        return Requests.indexRequest()
                .index(index)
                .type(type)
                .id(String.valueOf(position))
                .source(convertedElem);
    }

    /**
     * @param rank,           a LifeSpanRank object
     * @param runtimeContext, the runtime context used by Flink Elasticsearch connector
     * @param indexer,        the IndexRequests processor
     */
    @Override
    public void process(EmberLampLifeSpanRank rank, RuntimeContext runtimeContext, RequestIndexer indexer) {
        try {
            int count = 1;
            for (StreetLamp lamp : rank.getLamps()) {
                // passing positional argument to update the rank
                indexer.add(createIndexRequest(lamp, count));
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public EmberElasticsearchRankSinkFunction(String index, String type) {
        this.index = index;
        this.type = type;
    }
}
