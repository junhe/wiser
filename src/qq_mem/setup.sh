set +e

sudo apt-get install -y curl

# install dev tools
sudo apt-get install -y libtool autoconf

# install grpc
cd ~/
#git clone -b $(curl -L https://grpc.io/release) https://github.com/grpc/grpc
git clone -b v1.7.x https://github.com/grpc/grpc
cd grpc
git submodule update --init

make -j$(proc)
sudo make install

# install protobuf
cd third_party/protobuf
./autogen.sh
./configure
make -j$(proc)
sudo make install

# This is required to run performance benchmarks
sudo apt-get install -y libgflags-dev


# install new cmake
cd $HOME
wget https://cmake.org/files/v3.10/cmake-3.10.0-Linux-x86_64.tar.gz
tar xf cmake-3.10.0-Linux-x86_64.tar.gz
echo 'export PATH=$HOME/cmake-3.10.0-Linux-x86_64/bin:$PATH' >> $HOME/.bashrc

# install GLOG
# If you encounter the problem of GFLAGS_NAMESPACE is not defined, 
# re-install gflags according to the following article.
# https://github.com/google/glog/wiki/Installing-Glog-on-Ubuntu-14.04
cd $HOME
wget https://github.com/google/glog/archive/v0.3.5.tar.gz
tar xf v0.3.5.tar.gz
cd glog-0.3.5
./configure
make -j$(proc)
sudo make install


# get elasticsearch
cd $HOME
git clone git@github.com:junhe/elasticsearch-5.6.3.git

# install java
cd $HOME/flashsearch/scripts/
./install-java.sh


# install vmtouch
cd $HOME
git clone https://github.com/hoytech/vmtouch.git
cd vmtouch
make
sudo make install


# boost
sudo apt-get install -y libboost-all-dev


# lz4
sudo apt-get install -y liblz4-dev

# google perftools
sudo apt-get install -y google-perftools libgoogle-perftools-dev

# configure the shared libraries
sudo ldconfig

# ccache
sudo apt-get install -y ccache

# cgroup
sudo apt-get install -y cgroup-bin

sudo apt-get install -y sysstat

sudo apt-get install -y blktrace

# For elastic search
# https://www.elastic.co/guide/en/elasticsearch/reference/current/vm-max-map-count.html
sudo sysctl -w vm.max_map_count=262144



echo "*            hard   memlock           unlimited" | sudo tee -a /etc/security/limits.conf
echo "*            soft    memlock           unlimited" | sudo tee -a /etc/security/limits.conf
echo "WARNING: The commands above may not be executed"

echo "Now, run"
echo "source ~/.bashrc"

echo "If you are on client, build and copy libbloom to /usr/lib"
