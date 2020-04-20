FROM python:3.6-alpine3.10 as BUILD

RUN apk add --no-cache git postgresql-libs postgresql-dev gcc musl-dev libffi-dev make gnupg && \
    rm -rf /var/cache/apk/*

COPY requirements.txt /root/sdaauto/requirements.txt
COPY sda_orchestrator /root/sdaauto/sda_orchestrator
COPY setup.py /root/sdaauto

RUN pip install --upgrade pip && \
    pip install -r /root/sdaauto/requirements.txt && \
    pip install /root/sdaauto

FROM python:3.6-alpine3.10

LABEL maintainer "CSC Developers"
LABEL org.label-schema.schema-version="1.0"

RUN apk add --no-cache --update libressl postgresql-libs openssh-client supervisor

COPY --from=BUILD /usr/local/lib/python3.6/ usr/local/lib/python3.6/

COPY --from=BUILD /usr/local/bin/sdainbox /usr/local/bin/

COPY --from=BUILD /usr/local/bin/sdacomplete /usr/local/bin/

COPY --from=BUILD /usr/local/bin/webapp /usr/local/bin/

ADD supervisor.conf /etc/

ENTRYPOINT ["supervisord", "--configuration", "/etc/supervisor.conf"]