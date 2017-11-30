sudo apt-get install -y curl

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


# install new cmake
cd $HOME
wget https://cmake.org/files/v3.10/cmake-3.10.0-Linux-x86_64.tar.gz
tar xf cmake-3.10.0-Linux-x86_64.tar.gz
echo 'export PATH=$HOME/cmake-3.10.0-Linux-x86_64/bin:$PATH' >> $HOME/.bashrc

