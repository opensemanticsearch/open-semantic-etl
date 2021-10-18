ARG FROM=debian:buster
FROM ${FROM}

ENV DEBIAN_FRONTEND=noninteractive
ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1

RUN apt-get update && apt-get install --no-install-recommends --yes \
    build-essential \
    curl \
    file \
    libffi-dev \
    librabbitmq4 \
    libssl-dev \
    poppler-utils \
    pst-utils \
    python3-celery \
    python3-dateutil \
    python3-dev \
    python3-feedparser \
    python3-lxml \
    python3-pip \
    python3-pycurl \
    python3-pyinotify \
    python3-pysolr \
    python3-rdflib \
    python3-requests \
    python3-scrapy \
    python3-setuptools \
    python3-sparqlwrapper \
    python3-wheel \
    scantailor \
    tesseract-ocr \
#    tesseract-ocr-all \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY ./src/opensemanticetl/requirements.txt /usr/lib/python3/dist-packages/opensemanticetl/requirements.txt
# install Python PIP dependecies
RUN pip3 install -r /usr/lib/python3/dist-packages/opensemanticetl/requirements.txt

COPY ./src/opensemanticetl /usr/lib/python3/dist-packages/opensemanticetl
COPY ./src/tesseract-ocr-cache/tesseract_cache /usr/lib/python3/dist-packages/tesseract_cache
COPY ./src/tesseract-ocr-cache/tesseract_fake /usr/lib/python3/dist-packages/tesseract_fake
COPY ./src/open-semantic-entity-search-api/src/entity_linking /usr/lib/python3/dist-packages/entity_linking

COPY ./etc/opensemanticsearch /etc/opensemanticsearch

# add user
RUN adduser --system --disabled-password opensemanticetl

RUN mkdir /var/cache/tesseract
RUN chown opensemanticetl /var/cache/tesseract

USER opensemanticetl

# start Open Semantic ETL celery workers (reading and executing ETL tasks from message queue)
CMD ["/usr/bin/python3", "/usr/lib/python3/dist-packages/opensemanticetl/tasks.py"]
