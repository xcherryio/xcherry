FROM golang:latest

ENV CONFIG_PATH=''
ENV SRC_ROOT=''

COPY . /xcherry/
WORKDIR /xcherry/

RUN rm -rf xcherry-server  \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    netcat-traditional \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* && \
    make bins && \
    chmod +x /xcherry/script/start-server.sh

EXPOSE  8801
ENTRYPOINT ["/xcherry/script/start-server.sh"]
