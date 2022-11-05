# go-amqpx
Pure golang AMQP 1.0 for client and server applications.

## GO AMQPX Development
The purpose of this project is that develop into AMQP libraries in GO so that:
- My AMQP GO projects won't need the standard AMQP libraries (C++ libraries)
- My size of my containerized GO application remain **small**
- Only a subset of the AMQP spec will be implemented for demostration purposes

AMQP Spec is located [here](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-complete-v1.0-os.pdf)

## Developing
```bash
go mod tidy
go build ./...
go test ./...
```
