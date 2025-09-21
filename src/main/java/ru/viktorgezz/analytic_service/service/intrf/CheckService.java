package ru.viktorgezz.analytic_service.service.intrf;

import ru.viktorgezz.analytic_service.model.Check;

import java.util.List;

public interface CheckService {

    void saveChecks(List<Check> checks);
}
