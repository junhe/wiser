path=$1

find $path -type f -name 'out.csv' -exec sh -c 'cat {}' \;
