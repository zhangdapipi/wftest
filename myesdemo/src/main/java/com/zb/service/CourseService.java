package com.zb.service;

import com.zb.entity.Course;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class CourseService {

    @Autowired(required = false)
    private RestHighLevelClient restHighLevelClient;
    public List<Course> findCurseByPage(Integer index , Integer size, String key , String studymodel,Integer min , Integer max)throws Exception{
        List<Course> list = new ArrayList<>();
        //全部查询
        SearchRequest searchRequest =new SearchRequest("xc_course");
        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        searchSourceBuilder.from((index-1)*size);
        searchSourceBuilder.size(size);
        //创建bool组合查询对象
        BoolQueryBuilder boolQueryBuilder=new BoolQueryBuilder();
        if(key!=null){
            // 第一个条件
            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(key, "name", "description");
            multiMatchQueryBuilder.field("name",10);
            boolQueryBuilder.must(multiMatchQueryBuilder);
        }
        if(studymodel!=null) {
            //第二个条件
            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", studymodel);
            //将精准数据存储到filter
            boolQueryBuilder.filter(termQueryBuilder);
        }
        if(min!=null && max!=null) {
            //第三个条
            RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price").gte(min).lte(max);
            boolQueryBuilder.filter(rangeQuery);
            //将查询条件通过and的方法组合
        }
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
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String s =sourceAsMap.get("studymodel").toString();
            String name =  sourceAsMap.get("name").toString();
            String description=sourceAsMap.get("description").toString();;
            //            //获取高亮的结果数据
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
            Course c = new Course();
            c.setDescription(description);
            c.setName(name);
            c.setStudymodel(s);
            list.add(c);
        }
        return list;
    }
}
