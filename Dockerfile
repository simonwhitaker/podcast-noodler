FROM python:3.11-slim as build

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    # Poetry's configuration:
    POETRY_NO_INTERACTION=1 \
    POETRY_HOME='/usr/local' \
    POETRY_VERSION=1.8.2

RUN apt-get update && \
    apt-get install -y curl gcc && \
    curl -sSL https://install.python-poetry.org | python3 -

RUN mkdir /build /venv
WORKDIR /build

COPY pyproject.toml poetry.lock /build/

# Python installs its dependcies in different places. By using
# venv we can narrow it down to a single folder which is then
# copied to a final image. '--copies' option causes Python to
# copy its dependcies rather using symlinks.
RUN python -m venv --copies /venv

# Allow to pass additional arguments to Poetry, like '--no-dev'.
ARG POETRY_OPTIONS
RUN . /venv/bin/activate \
    && poetry install --no-interaction --no-ansi $POETRY_OPTIONS

FROM python:3.11-slim as runtime

# Use the same path in both stages; the build stage hard-codes its path to
# python in the shebang of dagster-webserver to the python in the venv, so we
# need python at the same path in the runtime image.
COPY --from=build /venv /venv

# Add our virtual environment to the path.
ENV PATH=/venv/bin:$PATH \
    PYTHONPATH='/venv'

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

WORKDIR /opt/dagster/app

# Copy your code and workspace to /opt/dagster/app
COPY podcast_noodler workspace.yaml /opt/dagster/app/

# Copy dagster instance YAML to $DAGSTER_HOME
# COPY dagster.yaml /opt/dagster/dagster_home/

EXPOSE 3000

ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
