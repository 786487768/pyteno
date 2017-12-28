#!/bin/bash
# run this main shell on the head node .
# MAX TIME must be set.
date
export HOME=/HOME/nsccgz_lli_1
export PYTHONHOME=$HOME/python3.6
export TENO_HOST=$HOSTNAME
echo $TENO_HOST
export TENO_PYTHON_PATH=$PYTHONHOME/bin
export TENO_REDIS_PATH=/WORK/app/redis/3.2.4/bin
#THHT_PATH
export PATH=$TENO_PYTHON_PATH:$TENO_PACKAGE_PATH:$HOME/Teno/experiment:$PATH
export PYTHONPATH=$HOME/Teno
######################################

# run redis-server
nohup $TENO_REDIS_PATH/redis-server  --protected-mode no &> log.redis &
###  check the redis-server is OK and put the settings into redis .
sleep 1

srun -N 1 -n 1 cal_time_job.py $TENO_HOST 6379 &> log.submmiter &

srun -N $[ SLURM_NNODES  ] -n $[ SLURM_NNODES  ] -c 24 python3 $HOME/Teno/common/executor.py 20 $TENO_HOST 6379 &> log.executor

killall -9 redis-server &> /dev/null
killall -9 python3 &> /dev/null
date