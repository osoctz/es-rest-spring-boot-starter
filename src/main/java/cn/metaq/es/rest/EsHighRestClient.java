package cn.metaq.es.rest;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Iterator;
import java.util.Set;


public class EsHighRestClient extends RestHighLevelClient {

    /**
     * 超时时间设为5分钟
     */
    private static final int TIME_OUT = 5 * 60 * 1000;

    public EsHighRestClient(Set<String> servers) {
        super(createRestClientBuilder(servers));
    }

    public EsHighRestClient(String servers) {
        super(createRestClientBuilder(servers));
    }

    private static RestClientBuilder createRestClientBuilder(Set<String> servers) {
        HttpHost[] httpHosts = new HttpHost[servers.size()];
        int i = 0;

        for (Iterator var3 = servers.iterator(); var3.hasNext(); ++i) {
            String server = (String) var3.next();
            HttpHost httpHost = HttpHost.create(server);
            httpHosts[i] = httpHost;
        }

        return RestClient.builder(httpHosts);
    }

    private static RestClientBuilder createRestClientBuilder(String servers) {
        String[] serverArray = servers.split(",");
        HttpHost[] httpHosts = new HttpHost[serverArray.length];

        for (int i = 0; i < serverArray.length; ++i) {
            HttpHost httpHost = HttpHost.create(serverArray[i]);
            httpHosts[i] = httpHost;
        }

        return RestClient.builder(httpHosts)
                .setMaxRetryTimeoutMillis(TIME_OUT)
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                            //超时时间5分钟
                            .setConnectTimeout(TIME_OUT)
                            //这就是Socket超时时间设置
                            .setSocketTimeout(TIME_OUT);
                    httpClientBuilder.setDefaultRequestConfig(requestConfigBuilder.build());
                    return httpClientBuilder;
                });
    }
}
