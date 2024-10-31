FROM alpine:3

RUN apk --no-cache add tzdata
COPY ./build/mc2mc /usr/local/bin/mc2mc

ENTRYPOINT ["/usr/local/bin/mc2mc"]
