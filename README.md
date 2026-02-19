## In memory Key value store

Simple in-memory key value store where data is being stored in file and supports GET,PUT,DEL command

### Commands to run

`go run main.go -cmd SET -key "user1" -value "raj"`

`go run main.go -cmd GET -key "user2" `

`go run main.go -cmd DEL -key "user1" `