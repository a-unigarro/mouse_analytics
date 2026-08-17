# Mouse Analytics

Project with **Apache Kafka**, using mouse-interaction data (e.g. movements, clicks) as the event stream.

## Overview

The goal of this project is to build a end-to-end pipeline where a **frontend** captures mouse events, sends them to a **backend**, and the backend publishes/consumes them through **Kafka**. It's meant as a hands-on sandbox to learn Kafka concepts (producers, consumers, topics, KRaft mode) rather than a production-ready analytics tool.

## Tech Stack

- **Apache Kafka** (`apache/kafka:4.3.1`) running in **KRaft mode**
- **Docker Compose** for local orchestration
- Backend and frontend services (see respective folders for details)

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose installed

### Run Kafka locally

```bash
docker compose up -d
```

This starts a single-node Kafka broker (KRaft mode) listening on `localhost:9092`.

### Backend / Frontend

See the `backend/` and `frontend/` directories for service-specific setup instructions.

## Status

This project is a toy/learning model and is evolving as new Kafka concepts are explored (topics, partitions, consumer groups, schema handling, etc.).

This project is under development
