FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ARG COMPONENT_DIR="ex-motherduck"

COPY /components/${COMPONENT_DIR}/pyproject.toml .

RUN uv pip install --system .

COPY /components/${COMPONENT_DIR} /code/

WORKDIR /code/

CMD ["python", "-u", "/code/src/component.py"]