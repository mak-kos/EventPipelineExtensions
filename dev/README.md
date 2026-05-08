# Local development helper

This `dev/` directory is **not part of your deliverable**. It exists only so you
can quickly verify that the supplied application code compiles and runs before
you start working on Docker, Kubernetes, and Helm.

## Bring up the infra
```bash
cd dev
docker compose up -d
```

## Run the apps
In one terminal:
```bash
cd services/event-api
# Windows
.\mvnw.cmd spring-boot:run
# MacOs
./mvnw spring-boot:run
```

In another:
```bash
cd services/event-processor-python
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux / macOS:
source .venv/bin/activate

pip install -r requirements.txt
python main.py
```

## Smoke test
```bash
curl -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"hello","value":42}'
```

You should see:
- A row in `events.events` (Postgres on port 5432)
- A new XML object in the `events` bucket (Minio console on http://localhost:9001)

Once the baseline works, switch focus to tasks described in BACKEND_TASKS.md
