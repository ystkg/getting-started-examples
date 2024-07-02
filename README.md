# ローカル開発環境

| Cloud | Dockerイメージ | Port | ライブラリ |
| --- | --- | ---: | --- |
| PostgreSQL | postgres | 5432 | github.com/lib/pq<br>github.com/jackc/pgx/v5 |
| MySQL | mysql | 3306 | github.com/go-sql-driver/mysql |
| MariaDB | mariadb | 3307 | github.com/go-sql-driver/mysql |
| Redis | redis | 6379 | github.com/redis/go-redis/v9 |
| Memcached | memcached | 11211 | github.com/<br>bradfitz/gomemcache/memcache |
| Cloud Storage | fsouza/fake-gcs-server | 4443 | cloud.google.com/go/storage |

## オブジェクトストレージ

### Cloud Storage

#### バケット

| 操作 | レシーバー | メソッド | パラメータ |
| --- | --- | --- | --- |
| 作成     | Gcs | CreateBucket | projectID, name |
| 削除     | Gcs | DeleteBucket | name |
| 存在確認 | Gcs | ExistsBucket | projectID, name |
| 一覧     | Gcs | Buckets      | projectID |

#### オブジェクト

| 操作 | レシーバー | メソッド | パラメータ |
| --- | --- | --- | --- |
| 書き込み | Gcs | Write  | bucket, name, contentType, bytes |
| 読み込み | Gcs | Read   | bucket, name |
| 削除     | Gcs | Delete | bucket, name |
| 存在確認 | Gcs | Exists | bucket, name |
| 一覧     | Gcs | List   | bucket, prefix |
