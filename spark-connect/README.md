# Spark-Connect

- `docker compose up -d` to starts a [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) server on localhost port `15002`.
- See platform build argument [docker-compose.yml](../docker-compose.yml) to build for a different platform other than Mac M1.
- S3 connection is bundled in. Credentials are in [.env](./.env)
- Delta doesn't seem to work well with dockerized Minio on Mac M1.