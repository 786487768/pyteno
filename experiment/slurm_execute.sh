#!/bin/bash
# run this main shell on the head node .
# MAX TIME must be set.
date
export HOME=/HOME/nsccgz_lli_1
export PYTHONHOME=$HOME/python3.6
export TENO_HOST=$HOSTNAME
echo $TENO_HOST
export TENO_PYTHON_PATH=$PYTHONHOME/bin
#THHT_PATH
export PATH=$TENO_PYTHON_PATH:$HOME/Teno/experiment:$PATH
######################################

for i in $(seq 1 $[ SLURM_NNODES ])
do
    srun -N 1 -n 1 -c 24 python3 slurm_execute.py 20 'htc.input' $i &> log.executor &
done

killall -9 python3 &> /dev/null
date