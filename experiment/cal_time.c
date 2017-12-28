#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>

int main(int argc, const char **argv)
{
    int i = 0, N_time = 0;
    long ex_time = 0;
    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);
    if(argc == 1)
        N_time = 1;
    else
        N_time = atoi(argv[1]);
    while(1){
        gettimeofday(&end_time, NULL);
        ex_time = end_time.tv_sec - start_time.tv_sec;
        if(ex_time >= N_time)
            break;
    }
    printf("ex_time = %ld\n", ex_time);
    return 0;
}
