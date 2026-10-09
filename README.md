# Dataset generator

This project generates reproducible random datasets of users and orders for the ECE Big Data Framework labs. It uses Python, [uv](https://docs.astral.sh/uv/) and Faker.

## Setup

```bash
uv sync
```

## Usage

```bash
uv run dataset-users -h
uv run dataset-users -c 2 -o jsonline
uv run dataset-orders -u 2 -C 1 -c 2 -o json
```

Generated CSV, JSON and Parquet datasets are intentionally excluded from Git.
