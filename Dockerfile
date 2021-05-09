# Compile stage
FROM golang:alpine AS build-env
ADD ./app /go/src/github.com/p4tin/goaws/app/
COPY ./go.mod /go/src/github.com/p4tin/goaws/
COPY ./go.sum /go/src/github.com/p4tin/goaws/
WORKDIR /go/src/github.com/p4tin/goaws
RUN GOARCH=amd64 go build -o /goaws app/cmd/goaws.go

# Final stage
FROM alpine
EXPOSE 4100
WORKDIR /
COPY --from=build-env /goaws /
COPY app/conf/goaws.yaml /conf/
CMD ["/goaws"]