FROM python:3.10 AS base

RUN apt-get update && apt-get install -y curl gnupg

# Install Google Cloud SDK (gsutil) non-interactively
# Using the official installation method with environment variables for non-interactive install
ENV CLOUDSDK_CORE_DISABLE_PROMPTS=1
ENV CLOUDSDK_INSTALL_DIR=/usr/local
RUN curl https://sdk.cloud.google.com | bash && \
    /usr/local/google-cloud-sdk/install.sh --quiet --usage-reporting=false && \
    /usr/local/google-cloud-sdk/bin/gcloud components install gsutil --quiet

ENV PATH="$PATH:/usr/local/google-cloud-sdk/bin"

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
RUN if [ -f requirements-dev.txt ]; then pip3 install -r requirements-dev.txt; fi
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


