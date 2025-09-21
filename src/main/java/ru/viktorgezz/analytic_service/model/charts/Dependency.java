package ru.viktorgezz.analytic_service.model.charts;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Dependency {
    private String from;
    private String to;
    private double correlation;
}
