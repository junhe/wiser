echo "this download may take couple hours(eg. 2hs in 20MB/s network)" 
mkdir wiki-trace
cd ./wiki-trace
wget -nd -r -l1 --no-parent http://www.wikibench.eu/wiki/2007-10/
wget -nd -r -l1 --no-parent http://www.wikibench.eu/wiki/2007-09/

rm ./index* 
rm README* MD5SUM*
echo "downloading finish, start unziping"
#gunzip -d ./*.gz

