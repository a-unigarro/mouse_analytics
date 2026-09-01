
## Tech stack

Apache Kafka 4.3.1 (KRaft mode) + Kafka UI · FastAPI + WebSockets · confluent-kafka · Pydantic · SQLAlchemy · PostgreSQL + pgAdmin · Docker Compose

## Kafka topics

| Topic | Partitions | Keyed by | Produced by | Consumed by |
|---|---|---|---|---|
| `mouse-events` | 3 | `session_id` | backend | `analytics-consumer` |
| `mouse-events-aggregated` | 3 | `session_id` / grid cell | `analytics-consumer` | `aggregate-writer` |

Topics are created explicitly by the `kafka-init` service on startup (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`), so partition counts are never left to Kafka's auto-create default.

**Listeners:** the broker exposes `localhost:9092` for processes on the host (e.g. the backend, run outside Docker) and `kafka:29092` for services on the Docker network. Inside a container, `localhost` refers to that container itself.

## Getting started

**1. Environment** — create a `.env` in the project root:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=mouse-events
KAFKA_CONSUMER_GROUP=analytics-consumer
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_AGGREGATE_TOPIC=mouse-events-aggregated


#### WINDOWED AGGREGATION CONFIG (Optional, if not defined default values used.)
WINDOW_SECONDS=10  ## 
HEATMAP_GRID_SIZE=50 ##

#### Second consumer  
KAFKA_CONSUMER_GROUP=aggregate-writer


#### DATABASE CONFIG
DB_USER=user_database
DB_PASSWORD=password_database
DB_NAME=mouse_analytics_db
DB_HOST=localhost
DB_PORT=5432


#### PGADMIN CONFIG
PGADMIN_EMAIL=example@example.com
PGADMIN_PASSWORD=123456
PGADMIN_PORT=8080
```

**2. Start the infrastructure:**

```bash
docker compose up -d
```
**## Scaling consumers**

Consumer services can be scaled horizontally using Docker Compose.

For example, to run three instances of the analytics consumer:
```bash
docker compose up -d --scale analytics-consumer=3
```

Brings up Kafka, creates both topics (`kafka-init`), the FastAPI backend (`api`), both consumer services (`analytics-consumer`, `aggregate-writer`), Postgres, pgAdmin, and Kafka UI. Check status with `docker compose ps`. Kafka-init should complete and exit, everything else should stay running.

**3. Start the backend:**

Already running as the `api` service (`http://localhost:8000`), with `--reload` enabled via a source volume mount. Only run it manually with the command below if you're working outside Docker:

```bash
pip install -r requirements.txt
uvicorn backend.main:app --reload
```

**4. Open the frontend** — `frontend/index.html` in a browser. Moving the mouse and clicking sends events through the whole pipeline.

**5. Inspect the results** — pgAdmin at `http://localhost:8080`, tables `session_click_rates` and `heatmap_cells`. Kafka UI at `http://localhost:8081` for topics, partitions, and consumer group lag. 

## Useful commands

```bash
# List topics
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Watch raw / aggregated events live
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic mouse-events --bootstrap-server localhost:9092
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic mouse-events-aggregated --bootstrap-server localhost:9092

# Logs for a specific service
docker compose logs kafka-init
docker compose logs db


# Check consumer groups**
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list


# Check a specific consumer group 

docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group group_name

```


## Useful commands

| Button | What it shows |
|---|---|
| **Click heatmap** | Polls `/api/heatmap?event_type=click` every 3s and overlays a spatial heatmap (pixel grid cells) of where clicks have landed on the page |
| **Movement heatmap** | Same overlay, but for `mousemove` events instead of clicks |
| **Tile clicks** | Polls `/api/elements?event_type=click` every 3s and shows a badge on each tile with its total click count |

All three are aggregated **across every session that's ever used the page**, not just your current one. The heatmap and tile-click views are independent and can be toggled on simultaneously, since they show different things (spatial position vs. which specific element).

