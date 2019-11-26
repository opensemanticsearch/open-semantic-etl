FROM debian:buster

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install --no-install-recommends --yes \
    curl \
    file \
    libssl-dev \
    libffi-dev \
    poppler-utils \
    pst-utils \
    python3-pycurl \
    python3-rdflib \
    python3-requests \
    python3-pysolr \
    python3-dateutil \
    python3-lxml \
    python3-feedparser \
    python3-celery \
    python3-pyinotify \
    python3-pip \
    python3-setuptools \
    python3-wheel \
    python3-dev \
    scantailor \
    tesseract-ocr \
    tesseract-ocr-all

COPY ./src /usr/lib/python3/dist-packages/

COPY ./etc/opensemanticsearch /etc/

# install Python PIP dependecies
RUN pip3 install -r /usr/lib/python3/dist-packages/opensemanticetl/requirements.txt

# add user
RUN adduser --system --disabled-password opensemanticetl

USER opensemanticetl

# start Open Semantic ETL celery workers (reading and executing ETL tasks from message queue)
CMD ["/usr/bin/python3", "/usr/lib/python3/dist-packages/opensemanticetl/tasks.py"]
