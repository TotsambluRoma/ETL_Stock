CREATE TABLE instruments (
    instrument_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticker text NOT NULL,
    company_name text NOT NULL,
    exchange text NOT NULL,
    currency char(3) NOT NULL,
    start_date date NOT NULL,
    active boolean NOT NULL,
    UNIQUE (exchange, ticker)
);

CREATE TABLE pipeline_runs (
    run_id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    operation text NOT NULL CHECK (operation IN ('etl', 'train')),
    status text NOT NULL CHECK (status IN ('running', 'success', 'no_changes', 'partial', 'failed')),
    details jsonb NOT NULL
);

CREATE TABLE daily_prices (
    instrument_id bigint NOT NULL REFERENCES instruments,
    trade_date date NOT NULL,
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume bigint NOT NULL CHECK (volume >= 0),
    adj_close double precision,
    dividends double precision,
    stock_splits double precision,
    run_id uuid NOT NULL REFERENCES pipeline_runs,
    raw_key text NOT NULL,
    PRIMARY KEY (instrument_id, trade_date),
    CHECK (low > 0 AND high < 'Infinity'::float8
           AND open BETWEEN low AND high AND close BETWEEN low AND high)
);

CREATE TABLE daily_features (
    instrument_id bigint NOT NULL,
    trade_date date NOT NULL,
    close double precision NOT NULL,
    return_1d double precision NOT NULL,
    sma_5 double precision NOT NULL,
    sma_20 double precision NOT NULL,
    avg_volume_20 double precision NOT NULL,
    volatility_20 double precision NOT NULL,
    PRIMARY KEY (instrument_id, trade_date),
    FOREIGN KEY (instrument_id, trade_date) REFERENCES daily_prices
);

CREATE TABLE model_versions (
    instrument_id bigint NOT NULL REFERENCES instruments,
    model_version uuid NOT NULL,
    artifact_uri text NOT NULL,
    dataset_uri text NOT NULL,
    mse_test double precision NOT NULL CHECK (mse_test >= 0 AND mse_test < 'Infinity'::float8),
    mse_baseline double precision NOT NULL CHECK (mse_baseline >= 0 AND mse_baseline < 'Infinity'::float8),
    is_active boolean NOT NULL DEFAULT false,
    PRIMARY KEY (instrument_id, model_version)
);

CREATE UNIQUE INDEX one_active_model
    ON model_versions (instrument_id) WHERE is_active;

CREATE TABLE daily_forecasts (
    instrument_id bigint NOT NULL,
    forecast_date date NOT NULL,
    model_version uuid NOT NULL,
    as_of_date date NOT NULL,
    predicted_close double precision NOT NULL
        CHECK (predicted_close > 0 AND predicted_close < 'Infinity'::float8),
    input_snapshot jsonb NOT NULL,
    issued_at timestamptz NOT NULL,
    PRIMARY KEY (instrument_id, forecast_date, model_version),
    FOREIGN KEY (instrument_id, as_of_date) REFERENCES daily_features,
    FOREIGN KEY (instrument_id, model_version) REFERENCES model_versions,
    CHECK (as_of_date < forecast_date)
);

CREATE VIEW forecast_comparison AS
SELECT i.ticker, f.as_of_date, f.forecast_date, f.model_version, f.issued_at,
       f.predicted_close, p.close AS actual_close
FROM daily_forecasts AS f
JOIN instruments AS i USING (instrument_id)
LEFT JOIN daily_prices AS p
    ON p.instrument_id = f.instrument_id AND p.trade_date = f.forecast_date;
