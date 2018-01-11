cd $HOME
wget https://github.com/antirez/redis/archive/4.0.2.tar.gz
tar xf 4.0.2.tar.gz
cd redis-4.0.2
make

echo 'export PATH=$HOME/redis-4.0.2/src/:$PATH' >> ~/.bashrc

echo "Now do:"
echo '$ source ~/.bashrc'

