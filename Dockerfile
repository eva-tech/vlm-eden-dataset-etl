FROM python:3.8 AS base

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

FROM base AS development

FROM base AS development-celery

FROM base AS development-celery-beat
