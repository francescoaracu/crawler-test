# crawler-playwright

## Run With Docker (With Persistent Data)

The repository includes a `docker-compose.yml` that mounts `./storage` from your host into the container at `/usr/src/app/storage`.
This means generated Crawlee data is available on your machine after the run.

# Option 1: Build and run

```sh
docker compose up --build
```

# Option 2: Run once and stop when complete

```sh
docker compose run --rm crawler
```
# Access data

Generated data will be available in:

- `storage/datasets/`
- `storage/key_value_stores/`
- `storage/request_queues/`

# Interrupt operations

Stop and remove the service container:

```sh
docker compose down
```

## Run locally (with python and pip)

To install dependencies, your can run the following command:

```sh
python -m pip install .
```

When the dependencies are installed, you may launch the crawler with:

```sh
python -m crawler_playwright
```


