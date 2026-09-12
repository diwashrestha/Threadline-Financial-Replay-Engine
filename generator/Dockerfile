# Event generator: writes JSON files to /data/events (shared volume in docker-compose).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Folders watched by Airflow FileSensor (raw / bad / processed).
# Owned by uid 50000 / gid 0 to match the Airflow image's default "airflow" user,
# so both containers can read/write/move files without world-writable permissions.
RUN mkdir -p /data/events/raw /data/events/bad /data/events/processed \
    && chown -R 50000:0 /data/events

COPY generator.py .

USER 50000:0

CMD ["python", "generator.py"]
