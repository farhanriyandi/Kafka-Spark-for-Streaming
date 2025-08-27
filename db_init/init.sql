CREATE TABLE weather_data (
    device_id VARCHAR(50),
    country VARCHAR(100),
    city VARCHAR(100),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_temp NUMERIC(5,2),
    avg_humidity NUMERIC(5,2)
);