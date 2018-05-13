wget https://redirector.gvt1.com/edgedl/go/go1.9.2.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.9.2.linux-amd64.tar.gz
echo "export GOPATH=$HOME/go" >> $HOME/.bashrc
echo export PATH=/usr/local/go/bin:\$PATH >> ~/.bashrc
echo export PATH=$GOPATH/bin/:\$PATH >> ~/.bashrc
mkdir $HOME/go
echo Now execute "source ~/.bashrc"

