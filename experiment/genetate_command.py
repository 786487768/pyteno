"""
生成每个节点需要执行的命令，写入到htc.input文件中，
形式为[[A..], [B..], ...]
"""
#!/usr/bin/env python3
import os
import sys
import json
import random
sys.path.append('..')
from common.jobs import Job
from common.tasks import TaskCrops, TaskCropsNormal, \
    TaskCropsFile, TaskCropsParams

def Job_t_1():
    job = Job()
    A = TaskCropsNormal(task_crops_id='A', job_id=job.job_id,
                        command='/home/ll/PycharmProjects/sleep_0', tasks_num=10000000)
    job.set_root_task_crops([A])
    return job

def Job_t_2():
    job = Job()
    cal_time = [1 for _ in range(10000)]
    cal_time_2 = [2 for _ in range(10000)]
    cal_time_4 = [4 for _ in range(10000)]
    cal_time_8 = [8 for _ in range(10000)]
    cal_time_16 = [16 for _ in range(10000)]
    cal_time_32 = [32 for _ in range(10000)]
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
    return job

def Job_e_1():
    # input_files_path = '/HOME/nsccgz_lli_1/vina_ex/input'
    # input_files_path = '/WORK/app/share/data-set4/cxcr4'
    input_files_path = '/home/ll/PycharmProjects/Teno/experiment'
    job = Job()
    # 分子对接应用
    # 读取输入文件，并创建输出文件
    input_files = os.listdir(input_files_path)
    output_file_path = os.getcwd()
    output_dir = os.path.join(os.getcwd(), "{id}_result".format(id=job.job_id))
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    input_files_new = [os.path.join(input_files_path, input_file)
                       for input_file in input_files]
    output_files = [os.path.join(output_dir, "%s.out" % input_file)
                    for input_file in input_files]
    C = TaskCropsParams(task_crops_id='C', job_id=job.job_id, exe_program_params=
    ['/WORK/app/AutoDock_Vina/1.1.2/bin/vina', '--cpu', '1', '--config', 'min.conf ',
     '--ligand', '/WORK/app/share/data-set4/cxcr4/ligand_pdbqt.decoys_final2164.mol2.pdbqt',
     '--receptor', 'receptor.pdbqt', '--out', 'result.pdbqt'],
                        replace_params_nums=3, replace_param_index=[6, 10],
                        replace_params=[input_files_new, output_files])
    job.set_root_task_crops(C)
    return job

if __name__ == '__main__':

    node_nums = int(sys.argv[1])
    job = Job_t_2()
    task_crops_info = job.bfs()
    commands = []
    for task_info in job.generator(task_crops_info):
        for task_command in task_info:
            commands.append(task_command.to_dict())
    separate_command = []
    for i in range(node_nums):
        separate_command.append([])
    for command in commands:
        node_index = random.randint(0, node_nums-1)
        separate_command[node_index].append(command)
    print(commands)
    print(len(commands))
    with open("htc.input", 'w+') as f:
        f.write(json.dumps(separate_command))
