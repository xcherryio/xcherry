FROM golang:latest

ENV CONFIG_TEMPLATE_PATH=''
ENV SRC_ROOT=''
ENV BACKEND_DEPENDENCY=${BACKEND_DEPENDENCY:-'temporal'}
ENV CONFIG_TEMPLATE_PATH=''
ENV SRC_ROOT=''
ENV HOST=''
ENV TEMPORAL_SERVICE_NAME=''
ENV CADENCE_SERVICE_NAME=''

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
