#!/usr/bin/env python3
import sys
sys.path.append('..')
from common.jobs import Job
from common.tasks import TaskCrops, TaskCropsNormal, \
    TaskCropsFile, TaskCropsParams


def Job_t_1(redis_hostname, redis_port):
    job = Job()
    A = TaskCropsNormal(task_crops_id='A', job_id=job.job_id,
                        command='/home/ll/PycharmProjects/sleep_0', tasks_num=10000000)
    job.set_root_task_crops([A])
    job.run(redis_hostname, redis_port)

def Job_t_2(redis_hostname, redis_port):
    job = Job()
    cal_time = ['1' for _ in range(10000)]
    cal_time_2 = ['2' for _ in range(10000)]
    cal_time_4 = ['4' for _ in range(10000)]
    cal_time_8 = ['8' for _ in range(10000)]
    cal_time_16 = ['16' for _ in range(10000)]
    cal_time_32 = ['32' for _ in range(10000)]
    cal_time.extend(cal_time_2)
    cal_time.extend(cal_time_4)
    cal_time.extend(cal_time_8)
    cal_time.extend(cal_time_16)
    cal_time.extend(cal_time_32)
    A = TaskCropsParams(task_crops_id='B', job_id=job.job_id,
                        exe_program_params=['/home/ll/PycharmProjects/cal_time', '1'],
                        replace_params_nums=1,
                        replace_param_index=[1],
                        replace_params=[cal_time])
    job.set_root_task_crops([A])
    job.run(redis_hostname, redis_port)

if __name__ == '__main__':
    redis_host = sys.argv[1]
    redis_port = sys.argv[2]
    Job_t_2(redis_host, redis_port)

