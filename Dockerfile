FROM golang:alpine3.14 as compiler

RUN apk add git

WORKDIR /app/build

COPY . .

RUN go build

FROM alpine:3.14

WORKDIR /app/prod

COPY --from=compiler /app/build/telegram-notifier .

ENTRYPOINT ["./telegram-notifier"]
