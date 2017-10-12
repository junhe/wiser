sudo apt-get install ant
wget http://mirrors.advancedhosters.com/apache//ant/ivy/2.4.0/apache-ivy-2.4.0-bin.tar.gz
tar xf apache-ivy-2.4.0-bin.tar.gz
mkdir -p ~/.ant/lib/
cp ./apache-ivy-2.4.0/ivy-2.4.0.jar ~/.ant/lib/


# install Jun's pyreuse library for utilities
git clone git@github.com:junhe/reuse.git
cd reuse
make install


