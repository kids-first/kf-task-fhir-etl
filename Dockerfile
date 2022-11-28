# build on base image
FROM python:3.7-stretch
LABEL maintainer="kids-first"

# set working directory
WORKDIR /app

# setup deps
RUN apt update && \
    apt install -y git gcc libpq-dev build-essential curl && \
    pip install --upgrade pip && \
    pip install setuptools wheel

# copy necessary files
COPY . /app

# install dependencies
RUN pip install -e .

# run on container start
ENTRYPOINT ["./scripts/entrypoint.sh"]
