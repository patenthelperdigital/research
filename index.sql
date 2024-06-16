-- Postgres --

docker run --name phd-postgres -e POSTGRES_USER=phd -e POSTGRES_PASSWORD=phd -d phd

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

COPY patent(reg_number, reg_date, appl_date, author_raw, owner_raw, address, name, actual, category, subcategory, kind, region, city, author_count)
FROM '/tmp/patents-demo-v2.csv'
DELIMITER ','
CSV HEADER;
docker cp patents-demo-v2.csv phd-postgres:/tmp

COPY person(kind, tax_number, full_name, active)
FROM '/tmp/persons-demo-v2.csv'
DELIMITER ','
CSV HEADER;

COPY ownership(patent_kind, patent_reg_number, person_tax_number)
FROM '/tmp/ownership-demo-v2.csv'
DELIMITER ','
CSV HEADER;


-- ClickHouse --
docker run -d --name phd-ch --ulimit nofile=262144:262144 -e CLICKHOUSE_DB=search -e CLICKHOUSE_USER=phd -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 -e CLICKHOUSE_PASSWORD=phd -p 8123:8123 -p 9000:9000/tcp clickhouse/clickhouse-server


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

cat /home/pavel/search-base.csv | docker exec -i phd-ch clickhouse-client --query='INSERT INTO search.search_base FORMAT CsvWithNames'

ALTER TABLE search.search_base ADD INDEX sbx(name) TYPE tokenbf_v1(4096, 3, 42);
ALTER TABLE search.search_base MATERIALIZE INDEX sbx;

SET allow_experimental_inverted_index = true;
ALTER TABLE search.search_base ADD INDEX inv_idx(name) TYPE full_text;
ALTER TABLE search.search_base MATERIALIZE INDEX inv_idx;


docker run -d --name test-ch -p 18123:8123 --ulimit nofile=262144:262144 test:latest
