package cn.metaq.es.rest;

import cn.metaq.es.util.IdUtils;
import cn.metaq.es.util.JsonUtils;
import lombok.extern.log4j.Log4j2;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static cn.metaq.es.constants.IndexType.DOC;


@Log4j2
public class ElasticsearchTemplate {

    private RestHighLevelClient client;

    private Integer batchNum;

    private ThreadPoolTaskExecutor executor;

    public ElasticsearchTemplate(RestHighLevelClient client, Integer batchNum, ThreadPoolTaskExecutor executor) {
        this.client = client;
        this.batchNum = batchNum;
        this.executor = executor;
    }

    public <T> List<T> search(Class<T> resultClass, QueryBuilder query, int offset, int limit) {

        return this.search(resultClass, query, offset, limit, null);
    }

    public <T> List<T> search(Class<T> resultClass, QueryBuilder query, int offset, int limit, String sortField, boolean asc) {

        return this.search(resultClass, query, offset, limit, (new FieldSortBuilder(sortField)).order(asc ? SortOrder.ASC : SortOrder.DESC));
    }

    public <T> List<T> search(Class<T> resultClass, QueryBuilder query, int offset, int limit, SortBuilder<?> sort) {

        String indexName = resultClass.getSimpleName().toLowerCase();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        if (query != null) {
            sourceBuilder.query(query);
        }

        if (sort != null) {
            sourceBuilder.sort(sort);
        }
        sourceBuilder.from(offset);
        sourceBuilder.size(limit);
        sourceBuilder.trackTotalHits(true);

        List<T> result = new ArrayList();
        SearchRequest searchRequest = new SearchRequest(new String[]{indexName});
        try {
            searchRequest.source(sourceBuilder);
            SearchResponse searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            int len = searchHits.length;

            for (int i = 0; i < len; ++i) {
                SearchHit hit = searchHits[i];
                String sourceAsString = hit.getSourceAsString();
                T entity = JsonUtils.fromJson(sourceAsString, resultClass);
                result.add(entity);
            }

            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <T> void bulkRequest(String index, List<T> data, RowMapper<T> rowMapper) throws Exception {

        BulkRequest bulkRequest = new BulkRequest();
        int count = 0;

        if (!CollectionUtils.isEmpty(data)) {

            Iterator<T> var1 = data.iterator();
            while (var1.hasNext()) {

                Map<String, Object> row = rowMapper.mapRow(var1.next());
                IndexRequest indexRequest = new IndexRequest(index, DOC.getValue(), IdUtils.objectId());

                bulkRequest.add(indexRequest.source(row));

                count++;
                //每batchNum 提交一次
                if (count % batchNum == 0) {

                    BulkResponse bulkResponse = this.client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    if (bulkResponse.hasFailures()) {
                        log.error("bulk failure,message={}", bulkResponse.buildFailureMessage());
                    }
                    //重新创建一个bulk
                    bulkRequest = new BulkRequest();
                }
            }

            //最后提交不足batchNum的部分
            if (count % batchNum > 0) {
                this.client.bulk(bulkRequest, RequestOptions.DEFAULT);
            }
        }

        log.info("total bulk request {}", count);
    }


    public <T> void asyncBulkRequest(String index, List<T> data, RowMapper<T> rowMapper) {

        if (!CollectionUtils.isEmpty(data)) {

            int len = data.size();
            //余数
            int num = len % batchNum;
            int count = len / batchNum;

            for (int i = 0; i < count; i++) {

                executor.execute(new BulkRequestTask<T>(client, index, data, rowMapper, i * batchNum, (i + 1) * batchNum));
            }

            if (num > 0) {

                executor.execute(new BulkRequestTask<T>(client, index, data, rowMapper, len - num, len));
            }

            log.info("total bulk request {}", len);
        }

    }

    /**
     * 计算总行数
     *
     * @param index
     * @param query
     * @return
     */
    public long count(String index, QueryBuilder query) {

        SearchRequest searchRequest = new SearchRequest(new String[]{index});
        try {

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            if (query != null) {
                sourceBuilder.query(query);
            }

            sourceBuilder.fetchSource(false);
            sourceBuilder.trackTotalHits(true);
            searchRequest.source(sourceBuilder);
            SearchResponse searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            return hits.getTotalHits();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 刷新索引
     *
     * @param index
     */
    public void refresh(String index) {

        RefreshRequest refreshRequest = new RefreshRequest(index);

        try {
            this.client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
