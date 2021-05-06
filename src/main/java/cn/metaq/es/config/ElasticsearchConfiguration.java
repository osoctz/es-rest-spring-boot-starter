package cn.metaq.es.config;

import cn.metaq.es.rest.ElasticsearchTemplate;
import cn.metaq.es.rest.EsHighRestClient;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadFactory;

@Configuration
@Log4j2
public class ElasticsearchConfiguration {

    @Value("${es.servers}")
    private String servers;

    @Value("${es.batch-num}")
    private Integer batchNum;

    @Bean(destroyMethod = "close")
    public EsHighRestClient esHighRestClient() {

        return new EsHighRestClient(servers);
    }

    @Bean
    public ElasticsearchTemplate elasticsearchTemplate(ThreadPoolTaskExecutor executor) {

        ThreadFactory elasticsearchTaskThreadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("es-task-%d")
                        .setUncaughtExceptionHandler((thread, e) -> log.error("线程{}异常信息:", thread.getName(), e))
                        .build();
        executor.setThreadFactory(elasticsearchTaskThreadFactory);
        return new ElasticsearchTemplate(esHighRestClient(), batchNum, executor);
    }
}
