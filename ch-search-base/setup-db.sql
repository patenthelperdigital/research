SET allow_experimental_inverted_index = true;

CREATE DATABASE search;

CREATE TABLE search.search_base (
    `name` String,
    `tax_number` String,
    `individual` Bool,
    `creation_date` Date,
    `active` Bool,
    `activity_code` String,
    `legal_address` String,
    `fact_address` String,
    INDEX full_text_idx(name) TYPE full_text(0),
    INDEX tokenbf_idx(name) TYPE tokenbf_v1(4096, 3, 42),
)
ENGINE = MergeTree()
ORDER BY name;

INSERT INTO search.search_base FROM INFILE '/tmp/search-base.csv' FORMAT CsvWithNames;
