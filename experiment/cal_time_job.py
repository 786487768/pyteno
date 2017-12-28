#!/usr/bin/env python3
import sys
sys.path.append('..')
from common.jobs import Job
from common.tasks import TaskCrops, TaskCropsNormal, \
    TaskCropsFile, TaskCropsParams

if __name__ == '__main__':

    redis_hostname = sys.argv[1]
    redis_port = sys.argv[2]
    A = TaskCropsNormal(task_crops_id='A', job_id='test_1',
                        command='/home/ll/PycharmProjects/cal_time 1', tasks_num=100)
    cal_time = Job()
    cal_time.set_root_task_crops([A])
    cal_time.run(redis_hostname, redis_port)
