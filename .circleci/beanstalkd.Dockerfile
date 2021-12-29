FROM alpine

RUN apk add --no-cache beanstalkd
RUN apk add --no-cache git bash openssh tar gzip ca-certificates

EXPOSE 11300
ENTRYPOINT ["/usr/bin/beanstalkd"]
