package com.minifms.incident.history;

import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Elasticsearch Java API Client 의 JSON 매퍼. Spring 의 ObjectMapper(JavaTimeModule 포함)를 복사해 써서
 * Instant 를 ISO-8601 로 주고받는다. 복사하는 이유: JacksonJsonpMapper 가 받은 매퍼의 설정을 바꾸기 때문이다.
 * 클라이언트(RestClient·ElasticsearchClient)는 spring.elasticsearch.* 로 Spring Boot 가 만든다.
 */
@Configuration
public class ElasticsearchConfig {

    @Bean
    public JsonpMapper jsonpMapper(ObjectMapper objectMapper) {
        return new JacksonJsonpMapper(objectMapper.copy());
    }
}
