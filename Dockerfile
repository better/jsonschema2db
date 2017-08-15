FROM postgres:9.6.3-alpine

# System setup
RUN addgroup -g 1000 app && adduser -D -G app -u 1000 app
ENV dir /home/app
ENV LC_ALL=en_US.utf-8

# System dependencies
RUN apk add --no-cache python3 python3-dev
RUN python3 -m ensurepip
RUN pip3 install --upgrade pip setuptools pytest

RUN apk add --no-cache libffi-dev \
libressl-dev \
gcc \
musl-dev \
git

# Global dependencies
COPY betterpy /opt/lib/betterpy/
RUN ln -Tfs /opt/lib/betterpy ${dir}/../betterpy

WORKDIR ${dir}

# App dependencies
COPY requirements.txt ${dir}/
RUN pip install -r requirements.txt

# File upload
COPY run.py schema.json test.py ${dir}/
COPY datalake_etl/ ${dir}/datalake_etl
COPY tables/ ${dir}/tables
RUN chown -R app: ${dir}

# App setup
USER app

CMD true
