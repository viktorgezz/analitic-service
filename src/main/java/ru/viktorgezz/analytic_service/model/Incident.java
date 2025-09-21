package ru.viktorgezz.analytic_service.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Incident {
    private UUID id;
    private String url;
    private OffsetDateTime startTime;
    private OffsetDateTime endTime;
    private Integer durationSeconds;
    private Integer failureCount;
}
