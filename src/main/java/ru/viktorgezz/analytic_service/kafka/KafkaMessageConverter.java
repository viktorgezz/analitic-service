package ru.viktorgezz.analytic_service.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.viktorgezz.analytic_service.model.Check;

import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
public class KafkaMessageConverter {

    private final ObjectMapper mapper;

    public KafkaMessageConverter() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(
                        DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                        false
                );
    }

    public List<Check> toChecks(String message) {
        try {
            return Arrays.asList(mapper.readValue(message, Check[].class));
        } catch (JsonProcessingException e) {
            log.error("Ошибка преобразования Kafka-сообщения в Check", e);
            throw new RuntimeException(e);
        }
    }
}

