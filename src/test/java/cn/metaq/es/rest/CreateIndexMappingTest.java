package cn.metaq.es.rest;

import lombok.extern.log4j.Log4j2;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import javax.annotation.Resource;

import java.io.IOException;

import static cn.metaq.es.constants.IndexType.DOC;

/**
 * @author zantang
 * @version 1.0
 * @description TODO
 * @date 2021/5/6 5:30 下午
 */
@Log4j2
public class CreateIndexMappingTest {

    @Resource
    private RestHighLevelClient client;

    @Test
    public void  testCreateMapping() throws IOException {

        String indexName = "testIndexName";
        GetIndexRequest indexRequest = new GetIndexRequest();
        indexRequest.indices(indexName);
        boolean indicesExists = client.indices().exists(indexRequest, RequestOptions.DEFAULT);

        if (indicesExists) {
            log.info("索引" + indexName + "已经存在");
            return;
        }
        //创建索引
        CreateIndexRequest request = new CreateIndexRequest(indexName)
                .settings(Settings.builder().put("refresh_interval", "-1"));//关闭索引自动刷新
        client.indices().create(request, RequestOptions.DEFAULT);

        //配置mapping
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(DOC.getValue())
                .startObject("properties")
                //标题
                .startObject("title").field("type", "text")
                .field("productName", "ik_smart")
                .field("search_analyzer", "ik_max_word").endObject()
                .endObject()
                .endObject()
                .endObject();

        PutMappingRequest mappingRequest = Requests.putMappingRequest(indexName).type(DOC.getValue()).source(mapping);
        client.indices().putMapping(mappingRequest, RequestOptions.DEFAULT);

    }
}
