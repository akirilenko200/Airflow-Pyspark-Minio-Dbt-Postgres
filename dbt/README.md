## Cooking dbt projects into docker images for Airflow
See [Dockerfile](pipeline/dbt/Dockerfile.exampledbt) for what is copied
- To build: `docker build --tag exampledbt:1 --build-arg build_for=linux/amd64 -f Dockerfile.exampledbt .`
- To run: `docker run -it --rm --network examplenet exampledbt:1 run` and optinally provide environment variables used in [profiles.yml](pipeline/dbt/profiles.yml)