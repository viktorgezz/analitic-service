package ru.viktorgezz.analytic_service.model.charts;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HeatmapEntry {
    private String day;
    private int hour;
    private int value;
}
