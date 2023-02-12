MERGE INTO {table} AS T
    USING {source} AS S
    ON T.air_quality_station = S.air_quality_station
        AND T.air_pollutant_code = S.air_pollutant_code
        AND T.measurement_ts = S.measurement_ts
    WHEN MATCHED AND (
                T.concentration <> S.concentration OR
                T.validity <> S.validity OR
                T.verification <> S.verification
        ) THEN
        UPDATE SET
            T.concentration = S.concentration,
            T.validity = S.validity,
            T.verification = S.verification,
            T._etl_timestamp = S._etl_timestamp
    WHEN NOT MATCHED
        THEN INSERT ROW;
