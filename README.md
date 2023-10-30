# ローカル開発環境

| Cloud | Dockerイメージ | Port | パッケージ |
| --- | --- | ---: | --- |
| PostgreSQL | postgres | 5432 | github.com/lib/pq<br>github.com/jackc/pgx/v5 |
| MySQL | mysql | 3306 | github.com/go-sql-driver/mysql |
| MariaDB | mariadb | 3307 | github.com/go-sql-driver/mysql |
| Redis | redis | 6379 | github.com/redis/go-redis/v9 |
| Memcached | memcached | 11211 | github.com/<br>bradfitz/gomemcache/memcache |
| BigQuery | ghcr.io/goccy/bigquery-emulator | 9050 | cloud.google.com/go/bigquery |
| Cloud Storage | fsouza/fake-gcs-server | 4443 | cloud.google.com/go/storage |
| Cloud Spanner | gcr.io/cloud-spanner-emulator/emulator | 9010<br>9020| cloud.google.com/go/spanner |
