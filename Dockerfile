# syntax=docker/dockerfile:1.6

# -----------------------------------------------------------------------------
# TSSI API + Playwright runtime image
# Built natively for Apple Silicon hosts via the linux/arm64 Playwright base.
# -----------------------------------------------------------------------------
FROM --platform=linux/arm64 mcr.microsoft.com/playwright/python:v1.47.0-jammy

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/srv \
    TZ=Asia/Bangkok

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    # The Playwright base image ships browsers pre-installed; this is a
    # defensive no-op that will only fetch missing channels.
    python -m playwright install --with-deps chromium

COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./alembic.ini

EXPOSE 8000

# Uvicorn entrypoint. `app.main:app` is created in Phase 1 as a minimal shell
# and will be expanded with scraper/calculation/API routes in later phases.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
