FROM golang:latest as builder
WORKDIR /go/src/github.com/gvialetto/pgsql-consul
COPY . .
RUN set -x \
  && go get -u github.com/golang/dep/cmd/dep \
  && dep ensure
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app .

FROM scratch
COPY --from=builder /go/src/github.com/gvialetto/pgsql-consul/app .
ENTRYPOINT ["./app"]
