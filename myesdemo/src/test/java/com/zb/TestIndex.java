package com.zb;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestIndex {

    @Autowired(required = false)
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void queryHight()throws Exception{
        //全部查询
        SearchRequest searchRequest =new SearchRequest("xc_course");
        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        // 第一个条件
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("基础领域CSS", "name", "description");
        multiMatchQueryBuilder.field("name",10);
        //第二个条件
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "201001");
        //第三个条
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price").gte(60).lte(100);
        //创建bool组合查询对象
        BoolQueryBuilder boolQueryBuilder=new BoolQueryBuilder();
        //将查询条件通过and的方法组合
        boolQueryBuilder.must(multiMatchQueryBuilder);
        //将精准数据存储到filter
        boolQueryBuilder.filter(termQueryBuilder);
        boolQueryBuilder.filter(rangeQuery);

        //资源与查询绑定
        searchSourceBuilder.query(boolQueryBuilder);

        //设置高亮对象
        HighlightBuilder highlightBuilder=new HighlightBuilder();
        highlightBuilder.preTags("<div style='color:red;'>");
        highlightBuilder.postTags("</div>");
        highlightBuilder.fields().add(new HighlightBuilder.Field("name"));
        highlightBuilder.fields().add(new HighlightBuilder.Field("description"));
        searchSourceBuilder.highlighter(highlightBuilder);

        //获取要显示的数
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","description"},new String[]{});
        //将绑定的信息存储到请求中
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest);
        SearchHits hits = response.getHits();
        SearchHit[] searchHits= hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            String index =hit.getIndex();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String studymodel =sourceAsMap.get("studymodel").toString();
            String name =  "";
            String description="";
            //获取高亮的结果数据
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(highlightBuilder!=null){
                HighlightField nameField= highlightFields.get("name");
                if(nameField!=null){
                    Text[] nameTxt = nameField.getFragments();
                    StringBuffer nameStr= new StringBuffer();
                    for (Text text:nameTxt) {
                        nameStr.append(text);
                    }
                    name=nameStr.toString();
                }
                HighlightField descriptionField= highlightFields.get("description");
                if(descriptionField!=null){
                    Text[] descriptionTxt = descriptionField.getFragments();
                    StringBuffer descriptionStr= new StringBuffer();
                    for (Text text:descriptionTxt) {
                        descriptionStr.append(text);
                    }
                    description=descriptionStr.toString();
                }
            }
            System.out.println(name+"\t"+studymodel+"\t"+id+"\t"+index+"\t"+description);
        }
    }


    @Test
    public void queryFilter()throws Exception{
        //全部查询
        SearchRequest searchRequest =new SearchRequest("xc_course");
        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        // 第一个条件
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("基础领域CSS", "name", "description");
        multiMatchQueryBuilder.field("name",10);
        //第二个条件
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "201001");
        //第三个条
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price").gte(60).lte(100);
        //创建bool组合查询对象
        BoolQueryBuilder boolQueryBuilder=new BoolQueryBuilder();
        //将查询条件通过and的方法组合
        boolQueryBuilder.must(multiMatchQueryBuilder);
        //将精准数据存储到filter
        boolQueryBuilder.filter(termQueryBuilder);
        boolQueryBuilder.filter(rangeQuery);

        //资源与查询绑定
        searchSourceBuilder.query(boolQueryBuilder);

        //获取要显示的数
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        //将绑定的信息存储到请求中
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest);
        SearchHits hits = response.getHits();
        SearchHit[] searchHits= hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            String index =hit.getIndex();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name =  sourceAsMap.get("name").toString();
            String studymodel =sourceAsMap.get("studymodel").toString();
            System.out.println(name+"\t"+studymodel+"\t"+id+"\t"+index);
        }
    }

    @Test
    public void queryBool()throws Exception{
        //全部查询
        SearchRequest searchRequest =new SearchRequest("xc_course");
        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        // 第一个条件
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("基础领域CSS", "name", "description");
        multiMatchQueryBuilder.field("name",10);
        //第二个条件
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "201001");

        //创建bool组合查询对象
        BoolQueryBuilder boolQueryBuilder=new BoolQueryBuilder();
        //将查询条件通过and的方法组合
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);

        //资源与查询绑定
        searchSourceBuilder.query(boolQueryBuilder);

        //获取要显示的数
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        //将绑定的信息存储到请求中
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest);
        SearchHits hits = response.getHits();
        SearchHit[] searchHits= hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            String index =hit.getIndex();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name =  sourceAsMap.get("name").toString();
            String studymodel =sourceAsMap.get("studymodel").toString();
            System.out.println(name+"\t"+studymodel+"\t"+id+"\t"+index);
        }
    }

    @Test
    public void queryMultiMatch()throws Exception{
        //全部查询
        SearchRequest searchRequest =new SearchRequest("xc_course");
        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        // MultiMatch查询多个列的信息
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("基础领域CSS", "name", "description");
        multiMatchQueryBuilder.field("name",10);
        //资源与查询绑定
        searchSourceBuilder.query(multiMatchQueryBuilder);
        //获取要显示的数
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        //将绑定的信息存储到请求中
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest);
        SearchHits hits = response.getHits();
        SearchHit[] searchHits= hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            String index =hit.getIndex();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name =  sourceAsMap.get("name").toString();
            String studymodel =sourceAsMap.get("studymodel").toString();
            System.out.println(name+"\t"+studymodel+"\t"+id+"\t"+index);
        }
    }

    @Test
    public void queryMatch()throws Exception{
        //全部查询
        SearchRequest searchRequest =new SearchRequest("xc_course");
        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        //match查询
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("description", "spring开发框架");
        matchQueryBuilder.operator(Operator.OR);
        matchQueryBuilder.minimumShouldMatch("80%");
        //资源与查询绑定
        searchSourceBuilder.query(matchQueryBuilder);
        //获取要显示的数
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        //将绑定的信息存储到请求中
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest);
        SearchHits hits = response.getHits();
        SearchHit[] searchHits= hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            String index =hit.getIndex();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name =  sourceAsMap.get("name").toString();
            String studymodel =sourceAsMap.get("studymodel").toString();
            System.out.println(name+"\t"+studymodel+"\t"+id+"\t"+index);
        }
    }

    @Test
    public void queryTerm()throws Exception{
        //全部查询
        SearchRequest searchRequest =new SearchRequest("xc_course");
        searchRequest.types("doc");
        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.termsQuery("name","java"));
        //SQL in () ==
        List<String> list = new ArrayList<String>();
        list.add("201002");
        list.add("201001");
        searchSourceBuilder.query(QueryBuilders.termsQuery("studymodel",list));
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest);
        SearchHits hits = response.getHits();
        SearchHit[] searchHits= hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            String index =hit.getIndex();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name =  sourceAsMap.get("name").toString();
            String studymodel =sourceAsMap.get("studymodel").toString();
            System.out.println(name+"\t"+studymodel+"\t"+id+"\t"+index);
        }
    }

    @Test
    public void queryAll()throws Exception{
        //全部查询
        SearchRequest searchRequest =new SearchRequest("xc_course");
        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        //分页条件
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest);
        SearchHits hits = response.getHits();
        SearchHit[] searchHits= hits.getHits();
        for (SearchHit hit : searchHits) {
            String id = hit.getId();
            String index =hit.getIndex();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
           String name =  sourceAsMap.get("name").toString();
           String studymodel =sourceAsMap.get("studymodel").toString();
            System.out.println(name+"\t"+studymodel+"\t"+id+"\t"+index);
        }
    }


    @Test
    public void deleteIndex() throws IOException {
        DeleteIndexRequest request=new DeleteIndexRequest("xc_course");
        DeleteIndexResponse deleteResponse= restHighLevelClient.indices().delete(request);
        System.out.println(deleteResponse.isAcknowledged());
    }
    @Test
    public void createIndex ()throws  Exception{
        CreateIndexRequest request=new CreateIndexRequest("xc_course", Settings.builder().put("number_of_shards","1").put("number_of_replicas","0").build());
        request.mapping("doc","{\n" +
                "\t\"properties\": {\n" +
                "\t\t\"description\": {\n" +
                "\t\t\t\"type\": \"text\",\n" +
                "\t\t\t\"analyzer\": \"ik_max_word\",\n" +
                "\t\t\t\"search_analyzer\": \"ik_smart\"\n" +
                "\t\t},\n" +
                "\t\t\"name\": {\n" +
                "\t\t\t\"type\": \"text\",\n" +
                "\t\t\t\"analyzer\": \"ik_max_word\",\n" +
                "\t\t\t\"search_analyzer\": \"ik_smart\"\n" +
                "\t\t},\n" +
                "\t\t\"pic\": {\n" +
                "\t\t\t\"type\": \"text\",\n" +
                "\t\t\t\"index\": false\n" +
                "\t\t},\n" +
                "\t\t\"price\": {\n" +
                "\t\t\t\"type\": \"float\"\n" +
                "\t\t},\n" +
                "\t\t\"studymodel\": {\n" +
                "\t\t\t\"type\": \"keyword\"\n" +
                "\t\t},\n" +
                "\t\t\"timestamp\": {\n" +
                "\t\t\t\"type\": \"date\",\n" +
                "\t\t\t\"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd\"\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}", XContentType.JSON);
        CreateIndexResponse response= restHighLevelClient.indices().create(request);
        System.out.println(response.isAcknowledged());
    }
    @Test
    public void addDoc() throws Exception{
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "spring cloud实战");
        jsonMap.put("description", "本课程主要从四个章节进行讲解： 1.微服务架构入门 2.spring cloud基础入门 3.实战Spring Boot 4.注册中心eureka。");
        jsonMap.put("studymodel", "201001");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        jsonMap.put("timestamp", dateFormat.format(new Date()));
        jsonMap.put("price", 5.6f);
        IndexRequest request= new IndexRequest("xc_course","doc");
        request.source(jsonMap);
        IndexResponse indexResponse= restHighLevelClient.index(request);
        DocWriteResponse.Result result = indexResponse.getResult();
        System.out.println(result);
    }
    @Test
    public void getDoc()throws Exception{
        GetRequest request = new GetRequest("xc_course","doc","_So4dXABy_inq7TDhhYQ");
        GetResponse getResponse= restHighLevelClient.get(request);
        Map<String, Object>map= getResponse.getSourceAsMap();
        System.out.println(map.get("name"));
    }
    @Test
    public void updateDoc () throws Exception{
        UpdateRequest request = new UpdateRequest("xc_course","doc","_So4dXABy_inq7TDhhYQ");
        Map<String , Object> param = new HashMap<>();
        param.put("name","spring boot实战");
        request.doc(param);
        UpdateResponse updateResponse= restHighLevelClient.update(request);
        System.out.println(updateResponse.status());
    }
    @Test
    public void deleteDoc() throws Exception{
        DeleteRequest deleteRequest=new DeleteRequest("xc_course","doc","_So4dXABy_inq7TDhhYQ");
        DeleteResponse deleteResponse= restHighLevelClient.delete(deleteRequest);
        System.out.println(deleteResponse.status());
    }

}
