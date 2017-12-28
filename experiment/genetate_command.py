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

if __name__ == '__main__':

    node_nums = int(sys.argv[1])
    # input_files_path = '/WORK/app/share/data-set4/cxcr4'
    input_files_path = '/home/ll/PycharmProjects/Teno/experiment'
    job = Job()
    A = TaskCropsNormal(task_crops_id='A', job_id=job.job_id,
                        command='/home/ll/PycharmProjects/cal_time 1', tasks_num=100)
    B = TaskCropsFile(task_crops_id='B',job_id=job.job_id,
                      exe_program='/home/ll/PycharmProjects/cal_time',
                      input_dir='/home/ll/PycharmProjects/Teno/experiment',
                      output_dir='/tmp', files_num=7)
    # 分子对接应用
    # 读取输入文件，并创建输出文件
    input_files = os.listdir(input_files_path)
    output_file_path = os.getcwd()
    output_dir = os.path.join(os.getcwd(), "{id}_result".format(id=job.job_id))
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    output_files = [os.path.join(output_dir, "%s.out" % input_file)
                    for input_file in input_files]
    C = TaskCropsParams(task_crops_id='B', job_id=job.job_id, exe_program_params=
        ['/WORK/app/AutoDock_Vina/1.1.2/bin/vina', '--cpu', '1' , '--config',  'min.conf ',
        '--ligand', '/WORK/app/share/data-set4/cxcr4/ligand_pdbqt.decoys_final2164.mol2.pdbqt',
        '--receptor', 'receptor.pdbqt', '--out', 'result.pdbqt'],
        replace_params_nums=3, replace_param_index=[6, 10],
        replace_params=[input_files, output_files])
    job.set_root_task_crops([C])
    task_crops_info = job.bfs()
    commands = []
    for task_info in job.generator(task_crops_info):
        for task_command in task_info:
            commands.append(task_command.command)
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
