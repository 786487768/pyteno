import os
import uuid
import queue
import json

from os.path import isfile, isdir, join


class TaskCropsType(object):
    TaskCrops_Normal = 0
    TaskCrops_File = 1
    TaskCrops_Para = 2
    TaskCrops_Other = -1


class TaskState(object):
    SUBMITTED = 0
    WAITING = 1
    RUNNING = 2
    SUCCESS = 3
    FAIL = 4
    PEND = 5
    CANCEL = 6
    SUBMIT_ERROR = -1

    STATE_DESCRIPTION = {SUBMITTED: "Your Task Has Been Submitted",
                         WAITING: "Your Task Is Waiting In Queue",
                         RUNNING: "Your Task Is Running",
                         SUCCESS: "Your Task Has Been Finished Successfully",
                         FAIL: "Your Task Has Been Finished Fail",
                         PEND: "Your Task is Pending",
                         CANCEL: "Your Task Has Been Canceled",
                         SUBMIT_ERROR: "Your Task Occurred Submit Error"}

    STATE_DESCRIPTION2 = {SUBMITTED: "Submitted",
                          WAITING: "Waiting",
                          RUNNING: "Running",
                          SUCCESS: "Success",
                          FAIL: "Fail",
                          PEND: "Pending",
                          CANCEL: "Cancel",
                          SUBMIT_ERROR: "Submit Error"}


class Task(object):

    def __init__(self, task_id=None, job_id=None, crops_id=None, dependence=[], command=None):
        if task_id:
            self.task_id = task_id
        else:
            self.task_id = str(uuid.uuid1()).split('-')[0]
        if job_id:
            self.job_id = job_id
        else:
            print("job id is none")
        if crops_id:
            self.crops_id = crops_id
        else:
            print("crops id is none")
        self.command = command
        self.state = TaskState.SUBMITTED
        self.dependence = dependence
        self.start_time = None
        self.end_time = None
        self.error_info = None
        self.level = -1

    def to_json_string(self):
        task_json_info = {'task_id': self.task_id,
                          'job_id': self.job_id,
                          'crops_id': self.crops_id,
                          'command': self.command,
                          'state': self.state,
                          'dependence': self.dependence,
                          'start_time': self.start_time,
                          'end_time': self.end_time,
                         }
        return json.dumps(task_json_info)


class TaskCrops(object):
    """任务团基类

    Attributes:
        task_crops_id: 任务团唯一id，可由用户指定也可自动生成（最好自动生成）
        job_id: 任务团所属作业id
        children: 下一步需要执行的任务团
        parents： 当前任务团的依赖节点
        level： 当前任务团在任务森林中所处的层次，根节点为0，下一层为1，以此类推
    """
    def __init__(self, task_crops_id=None, job_id=None):

        if task_crops_id:
            self.task_crops_id = task_crops_id
        else:
            self.task_crops_id = uuid.uuid1()
        if not job_id:
            print("job id is none")
        self.job_id = job_id
        self.children = list()
        self.parents = list()
        self.level = -1
        self.tasks_num = 0


class TaskCropsNormal(TaskCrops):
    """普通任务团（仅包含一个任务）

    Attributes:
        task_crops_id: 任务团唯一id，可由用户指定也可自动生成（最好自动生成）
        job_id: 任务团所属作业id
        command： 任务执行命令
    """
    def __init__(self, task_crops_id=None, job_id=None, command=None, tasks_num=1):

        super(TaskCropsNormal, self).__init__(task_crops_id, job_id)
        self.task_crops_type = TaskCropsType.TaskCrops_Normal
        self.command = command
        self.tasks_num = tasks_num


class TaskCropsFile(TaskCrops):
    """文件类型任务团

    Attributes:
        task_crops_id: 任务团唯一id，可由用户指定也可自动生成（最好自动生成）
        job_id: 任务团所属作业id
        input_files: 输入文件列表path
        output_files: 输出文件列表path
        input_dir: 输入文件目录path
        output_dir：输出文件目录path
        files_num：文件个数

        input_files和input_dir两者选其一赋值，
        output_files和output_dir都为空时，自动生成与输入文件对应的输出文件
    """
    def __init__(self, task_crops_id=None, job_id=None, exe_program=None,
                 input_files=None, output_files=None, input_dir=None,
                 output_dir=None, files_num=None):

        super(TaskCropsFile, self).__init__(task_crops_id, job_id)
        self.task_crops_type = TaskCropsType.TaskCrops_File
        self.exe_program = exe_program
        self.input_files = input_files
        self.output_files = output_files
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.tasks_num = files_num


class TaskCropsParams(TaskCrops):
    """参数类型任务团

    Attributes:
        task_crops_id: 任务团唯一id，可由用户指定也可自动生成（最好自动生成）
        job_id: 任务团所属作业id
        exe_program_params：可执行程序path和参数, 形式为[exe, param1， param2, param3, ...]
        replace_params_nums: 需要被替换的参数个数
        replace_params:      需要被替换的参数
        replace_param_index: 需要被替换的参数在exe_params中的索引
    """
    def __init__(self, task_crops_id=None, job_id=None, exe_program_params=[],
                 replace_params_nums=None, replace_params=[],replace_param_index=None):
        super(TaskCropsParams, self).__init__(task_crops_id, job_id)
        self.task_crops_type = TaskCropsType.TaskCrops_Para
        self.exe_program_params = exe_program_params
        self.replace_params_nums = replace_params_nums
        self.replace_params = replace_params
        self.replace_param_index = replace_param_index
