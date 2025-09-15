# Build commands

## golang
`protoc --go_out=./pkg/ ./proto/*.proto -I ./proto/`

## android
`protoc -I=./proto --java_out=./src/main/java/ --kotlin_out=./src/main/java/ ./proto/*.proto`
