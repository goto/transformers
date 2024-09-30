FROM alpine:3

RUN apk --no-cache add tzdata
COPY ./build/max2max /usr/local/bin/max2max

ENTRYPOINT ["/usr/local/bin/max2max"]
