package it.uniroma2.ember.elasticsearch;

import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;


public class EmberElasticsearchSinkFunction implements ElasticsearchSinkFunction<StreetLamp> {

    private String index = "";
    private String type = "";

    /**
     * This method can be used to create an IndexRequest
     *
     * @param element, the StreetLamp to store
     */
    private IndexRequest createIndexRequest(StreetLamp element) throws Exception {

        byte[] convertedElem = new ObjectMapper().writeValueAsBytes(element);

        // creating update request
        return Requests.indexRequest()
                .index(index)
                .type(type)
                .id(String.valueOf(element.getId()))
                .source(convertedElem);
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

    public EmberElasticsearchSinkFunction(String index, String type) {
        this.index = index;
        this.type = type;
    }
}
