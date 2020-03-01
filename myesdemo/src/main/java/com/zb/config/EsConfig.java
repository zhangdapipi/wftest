package com.zb.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsConfig {
    @Value("${my.es}")
    private String esHost;

    @Bean
    public RestHighLevelClient client() {
        String[] strs = esHost.split(",");
        HttpHost[] httpHosts = new HttpHost[strs.length];
        for (int i = 0; i < strs.length; i++) {
            String addr = strs[i];
            HttpHost httpHost = new HttpHost(addr.split(":")[0], Integer.parseInt(addr.split(":")[1]), "http");
            httpHosts[i] = httpHost;
        }
        return new RestHighLevelClient(RestClient.builder(httpHosts));
    }

}
