FROM node:20.18-bookworm-slim AS dashboard
WORKDIR /dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build

FROM python:3.11.10-slim-bookworm
RUN pip install --no-cache-dir uv==0.10.2 && useradd --system --uid 10002 api
WORKDIR /app/python
COPY python/pyproject.toml python/uv.lock ./
RUN uv sync --frozen --no-dev --extra api --no-install-project
COPY python/src ./src
COPY migrations /app/migrations
RUN uv sync --frozen --no-dev --extra api
COPY --from=dashboard /dashboard/dist /app/dashboard/dist
ENV DASHBOARD_DIST=/app/dashboard/dist PATH="/app/python/.venv/bin:$PATH"
USER api
CMD ["python", "-m", "polygemy.api", "serve"]
