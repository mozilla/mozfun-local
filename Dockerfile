ARG PYTHON_VERSION=3.10

# use buster image because the default bullseye image released 2021-08-17
# sha256:ffb6539b4b233743c62170989024c6f56dcefa69a83c4bd9710d4264b19a98c0
# has updated coreutils that require a newer linux kernel than provided by CircleCI, per
# https://forums.docker.com/t/multiple-projects-stopped-building-on-docker-hub-operation-not-permitted/92570/6
# and https://forums.docker.com/t/multiple-projects-stopped-building-on-docker-hub-operation-not-permitted/92570/11
# --platform=linux/amd64 added to prevent pulling ARM images when run on Apple Silicon
FROM --platform=linux/amd64 python:${PYTHON_VERSION}-slim-buster AS base

COPY . mozfun-local/
#RUN git clone git://github.com/yyuu/pyenv.git .pyenv

RUN cd mozfun-local && \
    python -m pip install --upgrade pip && \
    python -m pip install pip-tools && \
    pip-compile --allow-unsafe --generate-hashes && \
    python -m pip install -r requirements.txt && \
    apt-get update && \
    apt-get install -y curl build-essential && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    . "$HOME/.cargo/env" && \
    maturin build
    