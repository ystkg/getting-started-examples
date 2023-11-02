# ローカル開発環境

| Cloud | Dockerイメージ | Port | ライブラリ |
| --- | --- | ---: | --- |
| PostgreSQL | postgres | 5432 | github.com/lib/pq<br>github.com/jackc/pgx/v5 |
| MySQL | mysql | 3306 | github.com/go-sql-driver/mysql |
| MariaDB | mariadb | 3307 | github.com/go-sql-driver/mysql |
| Redis | redis | 6379 | github.com/redis/go-redis/v9 |
| Memcached | memcached | 11211 | github.com/<br>bradfitz/gomemcache/memcache |
| BigQuery | ghcr.io/goccy/bigquery-emulator | 9050 | cloud.google.com/go/bigquery |
| Cloud Storage | fsouza/fake-gcs-server | 4443 | cloud.google.com/go/storage |
| Cloud Spanner | gcr.io/cloud-spanner-emulator/emulator | 9010<br>9020| cloud.google.com/go/spanner |

## データウェアハウス

### BigQuery

## オブジェクトストレージ

### Cloud Storage

#### バケット

| 操作 | メソッド名 | パラメータ |
| --- | --- | --- |
|作成|CreateBucket|projectID, name|
|削除|DeleteBucket|name|
|存在確認|ExistsBucket|projectID, name|
|一覧|ListBuckets|projectID|

#### オブジェクト

| 操作 | メソッド名 | パラメータ |
| --- | --- | --- |
|書き込み|Write|bucket, name, contentType, bytes|
|読み込み|Read|bucket, name|
|削除|Delete|bucket, name|
|存在確認|Exists|bucket, name|
|一覧|List|bucket, prefix|

## NewSQL

### Cloud Spanner

