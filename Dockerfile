FROM python:3.10 AS base

RUN apt-get update

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN pip3 install --upgrade pip

WORKDIR /intelligence

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

ENV PYTHONUNBUFFERED 1

COPY . /intelligence

RUN chmod +x /intelligence/scripts/*.sh

RUN mkdir -p /var/lib/flower

FROM base AS development

FROM base AS development-celery-beat
FROM base AS staging-celery-beat
FROM base AS production-celery-beat

FROM base AS development-celery
RUN pip3 install -r requirements-dev.txt
ENTRYPOINT ["scripts/celery.sh"]

FROM base AS development-flower
ENTRYPOINT ["scripts/flower.sh"]

FROM base AS staging-celery
ENTRYPOINT ["scripts/celery.sh"]

FROM base AS staging-flower
ENTRYPOINT ["scripts/flower.sh"]

FROM base AS production-celery
ENTRYPOINT ["scripts/celery.sh"]

FROM base AS production-flower
ENTRYPOINT ["scripts/flower.sh"]


