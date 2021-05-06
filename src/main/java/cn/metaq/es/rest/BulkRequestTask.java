package cn.metaq.es.rest;

import cn.metaq.es.util.IdUtils;
import lombok.extern.log4j.Log4j2;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static cn.metaq.es.constants.IndexType.DOC;


@Log4j2
public class BulkRequestTask<T> implements Runnable {

    private RestHighLevelClient client;

    private String index;

    private List<T> data;

    private RowMapper<T> rowMapper;

    private int start;
    private int end;

    public BulkRequestTask(RestHighLevelClient client, String index, List<T> data, RowMapper<T> rowMapper, int start, int end) {
        this.client = client;
        this.index = index;
        this.data = data;
        this.rowMapper = rowMapper;
        this.start = start;
        this.end = end;
    }

    @Override
    public void run() {

        BulkRequest bulkRequest = new BulkRequest();

        if (!CollectionUtils.isEmpty(data)) {

            for (int i = start; i < end; i++) {

                Map<String, Object> row = rowMapper.mapRow(data.get(i));
                bulkRequest.add(new IndexRequest(index, DOC.getValue(), IdUtils.objectId()).source(row));
            }

            try {

                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                if (bulkResponse.hasFailures()) {
                    log.error("bulk failure,message={}", bulkResponse.buildFailureMessage());
                }
                //
                RefreshRequest refreshRequest = new RefreshRequest(index);
                this.client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("bulk failure", e);
            }

            log.info("bulk completed,[{}]rows", end - start);
        }
    }
}
