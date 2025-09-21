package ru.viktorgezz.analytic_service.model.charts;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TimestampValue {
    private String timestamp;
    private double value;
}
