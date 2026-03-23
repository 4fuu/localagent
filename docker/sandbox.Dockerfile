FROM mcr.microsoft.com/playwright/python:v1.52.0-noble

ENV DEBIAN_FRONTEND=noninteractive \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates git \
    && curl -fsSL https://deb.nodesource.com/setup_24.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir \
    "uv>=0.10.7,<0.11" \
    "playwright==1.52.0"

RUN node --version \
    && npm --version \
    && uv --version \
    && python -c "from playwright.sync_api import sync_playwright; print('playwright-ok')"

WORKDIR /workspace
