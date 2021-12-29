FROM golang:1.17-alpine
RUN apk add git bash openssh tar gzip ca-certificates gcc musl-dev