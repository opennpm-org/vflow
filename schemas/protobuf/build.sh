rm -fr build
mkdir -p build
cp vflow.proto build
cd build
protoc --proto_path=. -I=. --go_out=. vflow.proto
cp github.com/opennpm-org/vflow/producer/vflow.pb.go ../../../producer/vflow.pb.go
rm vflow.proto
