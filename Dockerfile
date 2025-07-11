FROM golang:latest

WORKDIR /app

COPY . . 

RUN go mod download

RUN go build -o main .

CMD ["/app/main"]
