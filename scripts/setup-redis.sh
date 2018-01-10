cd $HOME
wget https://github.com/antirez/redis/archive/4.0.2.tar.gz
tar xf 4.0.2.tar.gz
cd redis-4.0.2
make

echo "$HOME/redis-4.0.2/src/" >> ~/.bashrc

echo "Now do:"
echo '$ source ~/.bashrc'

