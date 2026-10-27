-- Run only through scripts/sharding/run.sh: target database guard is mandatory.
-- Each probe runs in its own transaction and is rolled back.
-- name: compatible_primary_key
CREATE TABLE stage5_work.cap_probe (id integer,k integer,PRIMARY KEY(id,k)) DISTRIBUTED BY (k);
-- name: incompatible_primary_key
CREATE TABLE stage5_work.cap_probe (id integer PRIMARY KEY,k integer) DISTRIBUTED BY (k);
-- name: random_heap
CREATE TABLE stage5_work.cap_probe (id integer,k integer) DISTRIBUTED RANDOMLY;
-- name: replicated_heap
CREATE TABLE stage5_work.cap_probe (id integer,k integer) DISTRIBUTED REPLICATED;
-- name: replicated_aoco
CREATE TABLE stage5_work.cap_probe (id integer,k integer) WITH (appendoptimized=true,orientation=column,compresstype=zstd) DISTRIBUTED REPLICATED;
