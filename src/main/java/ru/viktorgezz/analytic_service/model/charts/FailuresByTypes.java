package ru.viktorgezz.analytic_service.model.charts;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FailuresByTypes {
    private int critical;
    private int warning;
    private int resolved;
}
