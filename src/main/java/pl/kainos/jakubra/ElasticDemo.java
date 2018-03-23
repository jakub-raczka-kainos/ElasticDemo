package pl.kainos.jakubra;


import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ElasticDemo {
    private static final String SQL = "select product_id, product_name, category_name, product_price " +
            "from products p left join categories c on p.product_category_id=c.category_id limit 100";
    private static final String DB = "jdbc:mysql://192.168.57.3:3306/retail_db";
    private static final String DB_USER = "retail_dba";
    private static final String DB_PASS = "cloudera";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 9300;
    private static final String CLUSTER_NAME = "kainos_lab";


    public static void main( String[] args ) {
        ResultSet rs = fetchResultSet();
        List<JSONObject> jsonList = getJSONFromResultSet(rs);
        TransportClient client = prepareClient();
        sendBulkRequest(jsonList, client);
        search(client);
    }

    private static void sendBulkRequest(List<JSONObject> jsonList, TransportClient client) {
        BulkRequestBuilder bulkBuilder = client.prepareBulk();

        final String index = "product";
        for (JSONObject j : jsonList) {
            final String type = j.get("category_name").toString().toLowerCase();
            final String id = j.get("product_id").toString();
            bulkBuilder.add(client.prepareIndex(index, type, id).setSource(j.toJSONString(), XContentType.JSON));
        }
        try {
            BulkResponse bulkRes = bulkBuilder.execute().actionGet();
            if(bulkRes.hasFailures()){
                System.out.println("Bulk Request failure with error: " + bulkRes.buildFailureMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<JSONObject> getJSONFromResultSet(ResultSet rs) {
        List<JSONObject> list = new ArrayList<>();
        try {
            while (rs.next()) {
                JSONObject obj = new JSONObject();
                obj.put("product_id", rs.getString("product_id"));
                obj.put("product_name", rs.getString("product_name"));
                obj.put("category_name", rs.getString("category_name"));
                obj.put("product_price", rs.getString("product_price"));
                list.add(obj);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    private static ResultSet fetchResultSet() {
        ResultSet result = null;
        try {
            Connection con = DriverManager.getConnection(DB, DB_USER, DB_PASS);
            PreparedStatement Statement = con.prepareStatement(SQL);
            result= Statement.executeQuery();
        } catch (SQLException ex) {
            System.out.println(ex);
        }
        return result;
    }

    private static TransportClient prepareClient() {
        TransportClient client = null;
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", CLUSTER_NAME)
                    .build();

            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));
        } catch (Exception e) {
            System.out.println(e);
        }
        return client;
    }

    private static void search(TransportClient client) {
        SearchResponse response = client.prepareSearch("product")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("product_name", "Nike"))//query is static
                .setFrom(0).setSize(60).setExplain(true)
                .get();

        List<SearchHit> searchHits = Arrays.asList(response.getHits().getHits());
        List<JSONObject> results = new ArrayList<>();
        JSONParser parser = new JSONParser();

        searchHits.forEach(
                hit -> results.add(parse(parser, hit)));
        results.forEach(r -> System.out.println(JSONValue.toJSONString(r)));
    }

    private static JSONObject parse(JSONParser parser, SearchHit hit) {
        JSONObject obj = null;
        try {
            obj = (JSONObject) parser.parse(hit.getSourceAsString());
        } catch (ParseException e) {System.out.println(e);}
        return obj;
    }
}
