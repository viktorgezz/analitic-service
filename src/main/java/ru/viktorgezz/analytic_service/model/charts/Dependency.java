package ru.viktorgezz.analytic_service.model.charts;

import lombok.Data;

@Data
public class Dependency {
    private String from;
    private String to;
    private double correlation;
}
