# Build context: the repo root (`make images`).
FROM python:3.12-slim
WORKDIR /app
COPY apps/seeder/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY apps/seeder/seed_users.py .
USER 65534
CMD ["python", "-u", "seed_users.py"]
