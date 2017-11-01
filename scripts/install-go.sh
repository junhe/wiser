wget https://redirector.gvt1.com/edgedl/go/go1.9.2.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.9.2.linux-amd64.tar.gz
echo export PATH=\$PATH:/usr/local/go/bin >> ~/.bashrc

echo Now execute "source ~/.bashrc"

