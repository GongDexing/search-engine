package cn.net.communion.elasticsearch.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import cn.net.communion.elasticsearch.constant.ErrCode;
import cn.net.communion.elasticsearch.entity.SearchDesc;
import cn.net.communion.elasticsearch.helper.JsonObject;

@Configuration
@PropertySource("classpath:elasticsearch.properties")
public class Client implements EnvironmentAware {
    private Logger logger = Logger.getLogger(getClass());
    private TransportClient client;
    private Environment env;
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");

    @Override
    public void setEnvironment(Environment env) {
        this.env = env;
        String[] ips = env.getProperty("node.ips").split(",");
        String[] ports = env.getProperty("node.ports").split(",");
        if (ips.length != ports.length) {
            logger.error("ip number should match node number, please check properties file");
            System.exit(1);
        }
        Settings settings = Settings.builder().put("client.transport.sniff", false)
                .put("cluster.name", env.getProperty("cluster.name")).build();
        client = new PreBuiltTransportClient(settings);
        try {
            for (int index = 0; index < ips.length; index++) {
                client.addTransportAddress(new InetSocketTransportAddress(
                        InetAddress.getByName(ips[index]), Integer.valueOf(ports[index])));
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public String search(String index, SearchDesc desc) {
        String type = desc.getType();
        String keyword = desc.getKeyword();
        String fieldNames = env.getProperty(buildPropertyKey(index, type, "match"));
        // SearchHits hits = fieldNames == null || keyword == null || "".equals(keyword)
        // ? client.prepareSearch(index).setTypes(type).setFrom(desc.getFrom())
        // .setSize(desc.getSize()).get().getHits()
        // : client.prepareSearch(index).setTypes(type).setFetchSource(desc.getFields(), null)
        // .setQuery(QueryBuilders.multiMatchQuery(keyword, fieldNames.split(",")))
        // .setFrom(desc.getFrom()).setSize(desc.getSize()).get().getHits();
        try {
            SearchRequestBuilder srb =
                    client.prepareSearch(index).setTypes(type).setFrom(desc.getFrom())
                            .setSize(desc.getSize()).setFetchSource(desc.getFields(), null);
            SearchHits hits = (fieldNames == null || keyword == null || "".equals(keyword))
                    ? srb.get().getHits()
                    : srb.setQuery(QueryBuilders.multiMatchQuery(keyword, fieldNames.split(","))
                            .type(MultiMatchQueryBuilder.Type.MOST_FIELDS)).get().getHits();
            return convertToJson(hits);
        } catch (NoNodeAvailableException e) {
            e.printStackTrace();
            return convertToJson(ErrCode.No_Node_Available);
        }

    }

    // 综合最近三天的热门搜索词
    public String hotSearch(String type, int size) {
        Set<String> set = new HashSet<String>();
        String[] dates = lastThreeDate();
        int[] divides = {2, 3, 4};
        for (int index = 0; index < dates.length; index++) {
            try {
                Terms keywords =
                        client.prepareSearch("log-" + dates[index]).setTypes("search_" + type)
                                .addAggregation(AggregationBuilders.terms("keywords")
                                        .field("content.keyword").size(size / divides[index]))
                                .get().getAggregations().get("keywords");
                keywords.getBuckets().forEach(bucket -> {
                    set.add(bucket.getKeyAsString());
                });
            } catch (Exception e) {
                logger.error(dates[index] + " has not hot search keywords");
                continue;
            }
        }
        return new JsonObject().dataListString(ErrCode.Success, set);

    }

    private String convertToJson(int errcode) {
        return new StringBuilder("{\"errcode\":").append(errcode).append("}").toString();
    }

    private String convertToJson(SearchHits hits) {
        StringBuilder sb = new StringBuilder("{\"errcode\":0,\"data\":").append("{\"total\":")
                .append(hits.getTotalHits()).append(",\"items\":[");
        for (SearchHit hit : hits.hits()) {
            sb.append(hit.getSourceAsString()).append(",");
        }
        return sb.substring(0, hits.hits().length > 0 ? (sb.length() - 1) : sb.length()) + "]}}";
    }

    private String buildPropertyKey(String index, String type, String act) {
        return new StringBuilder(index).append(".").append(type).append(".").append(act)
                .append(".fields").toString();
    }

    public void shutdown() {
        client.close();
    }

    public String search(String type, String keyword, int from, int size) {
        String matchFields = env.getProperty(buildPropertyKey("data", type, "match"));
        String returnFields = env.getProperty(buildPropertyKey("data", type, "return"));
        try {
            SearchRequestBuilder srb = client.prepareSearch("data").setTypes(type).setFrom(from)
                    .setSize(size).setFetchSource(returnFields.split(","), null);
            SearchHits hits = (keyword == null || "".equals(keyword)) ? srb.get().getHits()
                    : srb.setQuery(QueryBuilders.multiMatchQuery(keyword, matchFields.split(","))
                            .type(MultiMatchQueryBuilder.Type.MOST_FIELDS)).get().getHits();
            return convertToJson(hits);
        } catch (NoNodeAvailableException e) {
            e.printStackTrace();
            return convertToJson(ErrCode.No_Node_Available);
        }
    }

    // 获得最近三天的热门搜索词
    private String[] lastThreeDate() {
        LocalDate ld = LocalDate.now();
        return new String[] {ld.format(formatter), ld.minusDays(1).format(formatter),
                ld.minusDays(2).format(formatter)};
    }
}
