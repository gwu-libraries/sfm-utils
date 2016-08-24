FROM python@sha256:ad39551743b356efda7c61f46019b97d49d1aab01b97f0e6d87c9b34326f3bfe
MAINTAINER Justin Littman <justinlittman@gwu.edu>

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    git=1:2.1.4-2.1+deb8u1 \
    zip=3.0-8

#pip set in 7.1.2
RUN pip install pip==7.1.2
#Avoid the warning of https
RUN pip install --upgrade ndg-httpsclient
RUN pip install appdeps
ENV SFM_REQS release
ENV DEBUG false
ADD setup_reqs.sh /opt/sfm-setup/
