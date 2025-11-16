# Real-Time Streaming Pipeline
## Data-source → Spark Streaming → PostgreSQL → Grafana

Streams CSV files via Spark, stores in PostgreSQL, visualized in Grafana.

---

## Architecture
![alt text](screenshots/image-6.png)

## Quick Start

### 1. Start Docker

**Screenshot: Docker containers running**
![alt text](screenshots/image-5.png)

---

## Run Pipeline

### Copy JARs

```bash
docker cp target/structured-streaming-app-1.0-SNAPSHOT.jar spark-master:/tmp/app.jar
docker cp libs/postgresql-42.7.1.jar spark-master:/tmp/postgresql-42.7.1.jar
```

### Start Spark Job

**Screenshot: Spark Streaming Job Running**
![alt text](screenshots/image-4.png)

---

### Add CSV Data

**Screenshot: Adding Data**
![alt text](screenshots/image-3.png)

---

### Verify PostgreSQL

```bash
docker exec -it postgres psql -U postgres -d mydb -c "SELECT * FROM my_table ORDER BY id;"
```
**Screenshot: PostgreSQL Verification**
![alt text](screenshots/image.png)
---

### Grafana Dashboard
![alt text](screenshots/image-2.png)

**Screenshot: Grafana Dashboard**
![alt text](screenshots/image-1.png)

---

**Made using Spark, PostgreSQL & Grafana**

