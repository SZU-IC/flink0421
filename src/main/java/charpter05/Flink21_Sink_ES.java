package charpter05;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink21_Sink_ES {
    public static void main(String[] args) {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
                .readTextFile("input/sensor-data.log");
//                .socketTextStream("localhost", 9999);

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop112",9200));
        httpHosts.add(new HttpHost("hadoop113",9200));
        httpHosts.add(new HttpHost("hadoop113",9200));

        //TODO 数据 Sink 到 ES
        final ElasticsearchSink<String> esSink = new ElasticsearchSink.Builder<String>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        //将数据放到 Map 中
                        HashMap<String, String> dataMap = new HashMap<>();
                        dataMap.put("data", element);
                        //创建 IndexRequest =》 指定 index ，指定 type ，指定source
                        final IndexRequest indexRequest = Requests.indexRequest("sensor0421")
                                .type("reading").source(dataMap);
                        // 添加到 Requestindexer
                        indexer.add(indexRequest);
                    }
                }
        ).build();

        inputDS.addSink(esSink);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
