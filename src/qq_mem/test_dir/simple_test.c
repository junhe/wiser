#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h> 
#include <fcntl.h>
#include <unistd.h>
int main() {
    int fd = open("./fdt", O_RDONLY);
    char * buff = (char *)malloc(10);
    int size = read(fd, buff, 10);

    printf("size : %d", size);
    printf("string: %s", buff);

    return 0;
}
