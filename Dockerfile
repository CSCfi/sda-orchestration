FROM python:3.7-alpine3.11 as BUILD

RUN apk add --no-cache git gcc musl-dev libffi-dev make gnupg && \
    rm -rf /var/cache/apk/*

COPY requirements.txt /root/sdaorch/requirements.txt
COPY sda_orchestrator /root/sdaorch/sda_orchestrator
COPY setup.py /root/sdaorch

RUN pip install --upgrade pip && \
    pip install -r /root/sdaorch/requirements.txt && \
    pip install /root/sdaorch

FROM python:3.7-alpine3.11

LABEL maintainer "NeIC System Developers"
LABEL org.label-schema.schema-version="1.0"

RUN apk add --no-cache --update supervisor

COPY --from=BUILD /usr/local/lib/python3.7/ usr/local/lib/python3.7/

COPY --from=BUILD /usr/local/bin/sdainbox /usr/local/bin/

COPY --from=BUILD /usr/local/bin/sdacomplete /usr/local/bin/

COPY --from=BUILD /usr/local/bin/sdaverified /usr/local/bin/

ADD supervisor.conf /etc/

RUN echo "nobody:x:65534:65534:nobody:/:/sbin/nologin" > passwd

USER 65534

ENTRYPOINT ["supervisord", "--configuration", "/etc/supervisor.conf"]