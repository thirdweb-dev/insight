FROM golang:1.23-alpine AS build

RUN apk add --no-cache git bash

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /bin/app -tags=production main.go

FROM alpine:3.18

RUN apk add --no-cache bash

COPY --from=build /bin/app /bin/app

EXPOSE 8080

ENTRYPOINT ["/bin/app"]

CMD []
