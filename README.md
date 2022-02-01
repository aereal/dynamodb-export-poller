[![status][ci-status-badge]][ci-status]
[![PkgGoDev][pkg-go-dev-badge]][pkg-go-dev]

# dynamodb-export-poller

You can [export DynamoDB table data to S3][dynamodb-export] but its job takes few minutes and currently it emits no events.

This tool just waits given DynamoDB table's export job finishes.

## Synopsis

```
go run github.com/aereal/dynamodb-export-poller/cmd/dynamodb-export-poller -table-arn arn:aws:...
```

Mandatory argument is only `-table-arn`.
Run `-help` and you can review other optional arguments.

## Installation

```sh
go install github.com/aereal/dynamodb-export-poller/cmd/dynamodb-export-poller
```

## License

See LICENSE file.

[pkg-go-dev]: https://pkg.go.dev/github.com/aereal/dynamodb-export-poller
[pkg-go-dev-badge]: https://pkg.go.dev/badge/aereal/dynamodb-export-poller
[ci-status-badge]: https://github.com/aereal/dynamodb-export-poller/workflows/CI/badge.svg?branch=main
[ci-status]: https://github.com/aereal/dynamodb-export-poller/actions/workflows/CI
[dynamodb]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html
[dynamodb-export]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataExport.html?sc_detail=blog_cta1 
