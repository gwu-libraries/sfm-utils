# Python 2.7
FROM python@sha256:8907ce99826e948f535e9e2524225a8c5b2d273f2b223c7fe7d82e1fb41efdc3
MAINTAINER Justin Littman <justinlittman@gwu.edu>

ARG DEBIAN_FRONTEND=noninteractive

# grab gosu for easy step-down from root
ENV GOSU_VERSION 1.10
RUN set -x \
	&& apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/* \
	&& wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" \
	&& wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc" \
	&& export GNUPGHOME="$(mktemp -d)" \
	&& gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
	&& gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
	&& rm -r "$GNUPGHOME" /usr/local/bin/gosu.asc \
	&& chmod +x /usr/local/bin/gosu \
	&& gosu nobody true

RUN apt-get update && apt-get install -y \
    git \
    zip

RUN pip install pip==10.0.1
#Avoid the warning of https
RUN pip install --upgrade ndg-httpsclient
RUN pip install appdeps
ENV SFM_REQS release
ENV DEBUG false
ADD setup_reqs.sh /opt/sfm-setup/
