package it.uniroma2.ember.elasticsearch;

import com.google.gson.Gson;
import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;


public class EmberElasticsearchSinkFunction implements ElasticsearchSinkFunction<StreetLamp> {

    /**
     * This method can be used to create an IndexRequest
     *
     * @param element, the StreetLamp to store
     */
    private IndexRequest createIndexRequest(StreetLamp element) throws Exception {

        Gson gson = new Gson();
        String json = gson.toJson(element);

        // creating update request
        String index = "ember";
        String type = "lamp";
        return Requests.indexRequest()
                .index(index)
                .type(type)
                .id(String.valueOf(element.getId()))
                .source(json.getBytes());
    }

    /**
     * @param element,        a StreetLamp object
     * @param runtimeContext, the runtime context used by Flink Elasticsearch connector
     * @param indexer,        the IndexRequests processor
     */
    @Override
    public void process(StreetLamp element, RuntimeContext runtimeContext, RequestIndexer indexer) {
        try {
            indexer.add(createIndexRequest(element));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public EmberElasticsearchSinkFunction(String index, String type) {
//        this.index = index;
//        this.type = type;
//    }
}
