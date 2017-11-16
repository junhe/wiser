# install grpc
cd /tmp/
git clone -b $(curl -L https://grpc.io/release) https://github.com/grpc/grpc
cd grpc
git submodule update --init

make
sudo make install

# install protobuf
cd third_party/protobuf
./autogen.sh
./configure
make
sudo make install

