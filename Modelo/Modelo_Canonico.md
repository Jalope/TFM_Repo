### 1. EUROSTAT_GOV10aEXP_VIVIENDA

| Campo                       | Tipo    | PK   |
| --------------------------- | ------- | ---- |
| `_id`                       | VARCHAR | ✓    |
| `geo`                       | VARCHAR |      |
| `time_period`               | INTEGER |      |
| `gov10a_exp_vivienda_share` | FLOAT   |      |

------

### 2. DEMOG_INDICATORS_VIEW

| Campo          | Tipo    | PK   |
| -------------- | ------- | ---- |
| `_id`          | VARCHAR | ✓    |
| `source`       | VARCHAR |      |
| `year`         | INTEGER |      |
| `region_level` | VARCHAR |      |
| `region_code`  | VARCHAR |      |
| `metric`       | VARCHAR |      |
| `scenario`     | VARCHAR |      |
| `value`        | FLOAT   |      |

------

### 3. INE_6507_POBLACION_NACIMIENTOS

| Campo           | Tipo    | PK   |
| --------------- | ------- | ---- |
| `_id`           | VARCHAR | ✓    |
| `region_code`   | VARCHAR |      |
| `year`          | INTEGER |      |
| `births`        | INTEGER |      |
| `births_male`   | INTEGER |      |
| `births_female` | INTEGER |      |

------

### 4. EUROSTAT_PEP_NUTS_RAW

| Campo         | Tipo    | PK   |
| ------------- | ------- | ---- |
| `_id`         | VARCHAR | ✓    |
| `geo`         | VARCHAR |      |
| `time_period` | INTEGER |      |
| `sex`         | VARCHAR |      |
| `age`         | VARCHAR |      |
| `values`      | FLOAT   |      |
| `unit`        | VARCHAR |      |
| `flags`       | VARCHAR |      |

------

### 5. SCENARIOS_METRICS

| Campo          | Tipo    | PK   |
| -------------- | ------- | ---- |
| `_id`          | VARCHAR | ✓    |
| `scenario`     | VARCHAR |      |
| `metric`       | VARCHAR |      |
| `year`         | INTEGER |      |
| `region_level` | VARCHAR |      |
| `region_code`  | VARCHAR |      |
| `value`        | FLOAT   |      |

------

### 6. INE_36652_POBLACION_PROJ_RAW

| Campo         | Tipo    | PK   |
| ------------- | ------- | ---- |
| `_id`         | VARCHAR | ✓    |
| `region_code` | VARCHAR |      |
| `year`        | INTEGER |      |
| `population`  | INTEGER |      |
| `lower_bound` | INTEGER |      |
| `upper_bound` | INTEGER |      |

------

### 7. MINISTERIO_VPO

| Campo         | Tipo    | PK   |
| ------------- | ------- | ---- |
| `_id`         | VARCHAR | ✓    |
| `region_code` | VARCHAR |      |
| `total_vpo`   | INTEGER |      |

------

### 8. INE_DEPENDENCY_PROJECTION

| Campo              | Tipo    | PK   |
| ------------------ | ------- | ---- |
| `_id`              | VARCHAR | ✓    |
| `region_code`      | VARCHAR |      |
| `year`             | INTEGER |      |
| `dependency_ratio` | FLOAT   |      |

------

### 9. EUROSTAT_SPENDING_FLOWS

| Campo                          | Tipo    | PK   |
| ------------------------------ | ------- | ---- |
| `_id`                          | VARCHAR | ✓    |
| `geo`                          | VARCHAR |      |
| `time_period`                  | INTEGER |      |
| `gf01_general_public_services` | FLOAT   |      |
| `gf07_health`                  | FLOAT   |      |
| `gf09_education`               | FLOAT   |      |
| `gf1002_old_age`               | FLOAT   |      |
| `d51c1`                        | FLOAT   |      |
| `d51c2`                        | FLOAT   |      |
| `d51c3`                        | FLOAT   |      |
| `d51d`                         | FLOAT   |      |
| `d51e`                         | FLOAT   |      |
| `d59`                          | FLOAT   |      |
| `d61`                          | FLOAT   |      |
| `d91`                          | FLOAT   |      |
| `inclusion_index`              | FLOAT   |      |
| `urban_rate`                   | FLOAT   |      |

------

### 10. DEMOG_DEPENDENCY_RATIO

| Campo              | Tipo    | PK   |
| ------------------ | ------- | ---- |
| `_id`              | VARCHAR | ✓    |
| `source`           | VARCHAR |      |
| `year`             | INTEGER |      |
| `region_code`      | VARCHAR |      |
| `dependency_ratio` | FLOAT   |      |

------

### 11. EUROSTAT_POP_NUTS_RAW

| Campo         | Tipo    | PK   |
| ------------- | ------- | ---- |
| `_id`         | VARCHAR | ✓    |
| `geo`         | VARCHAR |      |
| `time_period` | INTEGER |      |
| `sex`         | VARCHAR |      |
| `age`         | VARCHAR |      |
| `values`      | FLOAT   |      |
| `unit`        | VARCHAR |      |
| `flags`       | VARCHAR |      |

------

### 12. EUROSTAT_GOV10A_EXP_GDP_COMPARATIVA

| Campo                     | Tipo    | PK   |
| ------------------------- | ------- | ---- |
| `_id`                     | VARCHAR | ✓    |
| `geo`                     | VARCHAR |      |
| `time_period`             | INTEGER |      |
| `gdp_per_capita`          | FLOAT   |      |
| `gov10a_exp_gdp_pc_share` | FLOAT   |      |

------

### 13. INE_31304_POBLACION_RAW

| Campo         | Tipo    | PK   |
| ------------- | ------- | ---- |
| `_id`         | VARCHAR | ✓    |
| `region_code` | VARCHAR |      |
| `year`        | INTEGER |      |
| `total`       | INTEGER |      |
| `male`        | INTEGER |      |
| `female`      | INTEGER |      |
| `density`     | FLOAT   |      |
| `urban_share` | FLOAT   |      |
| `rural_share` | FLOAT   |      |

------

### 14. EUROSTAT_GOV10A_EXP_GDP

| Campo                  | Tipo    | PK   |
| ---------------------- | ------- | ---- |
| `_id`                  | VARCHAR | ✓    |
| `geo`                  | VARCHAR |      |
| `time_period`          | INTEGER |      |
| `gov10a_exp_gdp_share` | FLOAT   |      |

------

### 15. MODERNIZATION_METRICS

| Campo          | Tipo    | PK   |
| -------------- | ------- | ---- |
| `_id`          | VARCHAR | ✓    |
| `country_code` | VARCHAR |      |
| `year`         | INTEGER |      |
| `metric`       | VARCHAR |      |
| `unit`         | VARCHAR |      |
| `value`        | FLOAT   |      |
| `source`       | VARCHAR |      |
| `notes`        | VARCHAR |      |
| `flags`        | VARCHAR |      |

------

### 16. INE_6548_POBLACION_DEFUNCIONES_RAW

| Campo           | Tipo    | PK   |
| --------------- | ------- | ---- |
| `_id`           | VARCHAR | ✓    |
| `region_code`   | VARCHAR |      |
| `year`          | INTEGER |      |
| `deaths`        | INTEGER |      |
| `deaths_male`   | INTEGER |      |
| `deaths_female` | INTEGER |      |
| `crude_rate`    | FLOAT   |      |

------

### 17. INE_4583_PERFIL_HOGAR

| Campo                | Tipo    | PK   |
| -------------------- | ------- | ---- |
| `_id`                | VARCHAR | ✓    |
| `region_code`        | VARCHAR |      |
| `n_households`       | INTEGER |      |
| `avg_household_size` | INTEGER |      |