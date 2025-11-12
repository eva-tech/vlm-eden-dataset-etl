FROM python:3.10 AS base

RUN apt-get update && apt-get install -y curl gnupg git maven default-jdk

# Install dcm4che fork from GitHub
RUN git clone https://github.com/eva-tech/dcm4che.git /tmp/dcm4che && \
    cd /tmp/dcm4che && \
    if [ -d "dcm4che-tool/dcm4che-tool-dcm2jpg" ]; then \
        cd dcm4che-tool/dcm4che-tool-dcm2jpg && \
        mvn clean package -DskipTests -Dmaven.test.skip=true || true; \
    elif [ -d "tools/dcm2jpg" ]; then \
        cd tools/dcm2jpg && \
        mvn clean package -DskipTests -Dmaven.test.skip=true || true; \
    else \
        mvn clean install -DskipTests -Dmaven.test.skip=true -Dmaven.javadoc.skip=true || true; \
    fi && \
    cd /tmp/dcm4che && \
    DCM2JPG_DIR=$(find . -type d -name "dcm2jpg*" -path "*/target/*" | head -1) && \
    DCM2JPG_JAR=$(find . -name "*dcm2jpg*.jar" -path "*/target/*" ! -name "*-sources.jar" ! -name "*-javadoc.jar" | head -1) && \
    if [ -n "$DCM2JPG_DIR" ] && [ -d "$DCM2JPG_DIR/bin" ]; then \
        cp -r "$DCM2JPG_DIR" /usr/local/dcm2jpg && \
        chmod +x /usr/local/dcm2jpg/bin/*.sh 2>/dev/null || true; \
    elif [ -n "$DCM2JPG_JAR" ]; then \
        cp "$DCM2JPG_JAR" /usr/local/dcm2jpg.jar && \
        DCM4CHE_DIR=$(dirname "$(dirname "$DCM2JPG_JAR")") && \
        cd "$DCM4CHE_DIR" && \
        mvn dependency:copy-dependencies -DoutputDirectory=/usr/local/dcm2jpg-lib -DskipTests -q 2>/dev/null || true && \
        echo '#!/bin/bash' > /usr/local/bin/dcm2jpg && \
        echo 'java -cp "/usr/local/dcm2jpg.jar:/usr/local/dcm2jpg-lib/*" org.dcm4che3.tool.dcm2jpg.Dcm2Jpg "$@"' >> /usr/local/bin/dcm2jpg && \
        chmod +x /usr/local/bin/dcm2jpg; \
    fi && \
    rm -rf /tmp/dcm4che

ENV PATH="$PATH:/usr/local/dcm2jpg/bin:/usr/local/bin"

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


