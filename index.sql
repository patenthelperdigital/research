CREATE TABLE search_index (
    name text,
    tax_number text,
    individual bool,
    creation_date date,
    active bool,
    activity_code text,
    region text,
    municipality text,
    settlement text,
    street text
);

COPY search_index(name, tax_number, individual, creation_date, active, activity_code, region, municipality, settlement, street)
FROM '/tmp/search_index.csv'
DELIMITER ','
CSV HEADER;

CREATE INDEX search_index_name ON search_index USING GIST(name gist_trgm_ops);

ALTER TABLE search_index ADD COLUMN ts tsvector 
    GENERATED ALWAYS AS (to_tsvector('russian', name)) STORED;

CREATE INDEX ts_idx ON search_index USING GIN (ts);