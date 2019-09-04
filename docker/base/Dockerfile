# Python 3.6
FROM python@sha256:81782ff0fadf910a9f13747608f0d93e8938cbbdc1e50dc8c88e4e1ab8d9e7b2
MAINTAINER Social Feed Manager <sfm@gwu.edu>

ARG DEBIAN_FRONTEND=noninteractive

# grab gosu for easy step-down from root
ENV GOSU_VERSION 1.11
RUN set -x \
	&& apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/* \
	&& wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" \
	&& wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc" \
	&& export GNUPGHOME="$(mktemp -d)" \
	&& gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
	&& gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
	&& command -v gpgconf && gpgconf --kill all || : \
	&& rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc \
	&& chmod +x /usr/local/bin/gosu \
	&& gosu nobody true

RUN apt-get update && apt-get install -y \
    git \
    zip

#Avoid the warning of https
RUN pip install --upgrade ndg-httpsclient
RUN pip install appdeps==1.1.0
ENV SFM_REQS release
ENV DEBUG false
ADD setup_reqs.sh /opt/sfm-setup/
