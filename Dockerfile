FROM golang:latest

ENV CONFIG_PATH=''
ENV SRC_ROOT=''

COPY . /xdb/
WORKDIR /xdb/

RUN rm -rf xdb-server  \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    netcat-traditional \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* && \
    make bins && \
    chmod +x /xdb/script/start-server.sh

EXPOSE  8801
ENTRYPOINT ["/xdb/script/start-server.sh"]
