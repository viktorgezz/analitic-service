-- Таблица для хранения результатов проверок
CREATE TABLE checks
(
    url String,
    timestamp DateTime('UTC'),
    success Bool,
    error Nullable(String),
    response_time Float32,
    status_code UInt16,
    content_type Nullable(String),
    content_length Nullable(UInt32),
    headers Map(String, String),
    is_https Bool,
    redirect_chain Array(String),
    security_headers Map(String, String),
    technology_stack Array(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (url, timestamp);
