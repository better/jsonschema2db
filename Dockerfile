FROM postgres:9.6.3-alpine

# System setup
RUN addgroup -g 1000 app && adduser -D -G app -u 1000 app
ENV dir /home/app
ENV LC_ALL=en_US.utf-8

# System dependencies
RUN apk add --no-cache python3 python3-dev
RUN pip3 install --upgrade setuptools pytest

RUN apk add --no-cache libffi-dev \
libressl-dev \
gcc \
musl-dev \
git

USER app

# Postgres setup
ENV PGDATA /home/app/postgres
RUN initdb

WORKDIR ${dir}

# File upload
COPY setup.py jsonschema2db.py ${dir}/
COPY test/ ${dir}/test

# App setup
RUN python3 setup.py install --user

# Run test
CMD pg_ctl -w start && createdb jsonschema2db-test && py.test test/test.py
