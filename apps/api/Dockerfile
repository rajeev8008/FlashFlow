FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml alembic.ini ./
COPY alembic ./alembic
COPY src ./src
COPY tests ./tests
RUN pip install --no-cache-dir .
ENV PYTHONUNBUFFERED=1
