FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ARG COMPONENT_DIR="wr-motherduck"

WORKDIR /code/

COPY /components/${COMPONENT_DIR}/pyproject.toml .
COPY /components/${COMPONENT_DIR}/uv.lock .

ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
RUN uv sync --all-groups --frozen

COPY /components/${COMPONENT_DIR} /code/

CMD ["python", "-u", "/code/src/component.py"]