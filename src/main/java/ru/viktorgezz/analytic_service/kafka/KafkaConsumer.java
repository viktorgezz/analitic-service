package ru.viktorgezz.analytic_service.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.viktorgezz.analytic_service.model.Check;
import ru.viktorgezz.analytic_service.service.intrf.CheckService;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private final KafkaMessageConverter converter;
    private final CheckService checkService;

    @KafkaListener(topics = "endpoint_test_results", groupId = "analytic-service")
    public void consume(String message) {
        log.info("Получено сообщение из Kafka: {}", message);
        List<Check> checks = converter.toChecks(message);
        checkService.saveChecks(checks);
    }
}


