FROM clickhouse/clickhouse-server:latest

COPY ./search-base.csv /tmp/

COPY ./setup-db.sql /docker-entrypoint-initdb.d/
