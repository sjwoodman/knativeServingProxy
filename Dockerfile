FROM centos:7
MAINTAINER Simon Woodmna <swoodman@redhat.com>
ARG BINARY=./knativeproxy

COPY ${BINARY} /opt/knativeproxy
ENTRYPOINT ["/opt/knativeproxy"]