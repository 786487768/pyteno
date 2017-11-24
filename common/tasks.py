import os
import uuid
import queue

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

    STATE_DESCRIPTION = {SUBMITTED: "Your Job Has Been Submitted",
                         WAITING: "Your Job Is Waiting In Queue",
                         RUNNING: "Your Job Is Running",
                         SUCCESS: "Your Job Has Been Finished Successfully",
                         FAIL: "Your Job Has Been Finished Fail",
                         PEND: "Your Job is Pending",
                         CANCEL: "Your Job Has Been Canceled",
                         SUBMIT_ERROR: "Your Job Occurred Submit Error"}

    STATE_DESCRIPTION2 = {SUBMITTED: "Submitted",
                          WAITING: "Waiting",
                          RUNNING: "Running",
                          SUCCESS: "Success",
                          FAIL: "Fail",
                          PEND: "Pending",
                          CANCEL: "Cancel",
                          SUBMIT_ERROR: "Submit Error"}


class Task(object):

    def __init__(self, task_id=None, job_id=None, crops_id=None, command=None):
        if task_id:
            self.task_id = task_id
        else:
            self.task_id = uuid.uuid1()
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
        self.start_time = None
        self.end_time = None
        self.error_info = None
        self.level = -1


class TaskCrops(object):
    '''任务团基类

    Attributes:
        task_crops_id: 任务团唯一id，可由用户指定也可自动生成（最好自动生成）
        job_id: 任务团所属作业id
        children: 下一步需要执行的任务团
        parents： 当前任务团的依赖节点
        level： 当前任务团在任务森林中所处的层次，根节点为0，下一层为1，以此类推
    '''
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

    @staticmethod
    def bfs(root_task_crops):
        task_crops = list()
        tag = dict()
        task_crops_queue = queue.Queue()
        # 根任务团的level设置为0
        root_task_crops.level = 0
        task_crops_queue.put(root_task_crops)
        tag[root_task_crops.task_crops_id] = 1
        while not task_crops_queue.empty():
            current_task_crops = task_crops_queue.get()
            task_crops.append(current_task_crops)
            print("%s %d" % (current_task_crops.task_crops_id, current_task_crops.level))
            for child_task_crops in current_task_crops.children:
                child_task_crops.level = current_task_crops.level + 1
                child_task_crops.parents.append(current_task_crops)
                if child_task_crops.task_crops_id not in tag:
                    task_crops_queue.put(child_task_crops)
                    tag[child_task_crops.task_crops_id] = 1
        return task_crops

    @staticmethod
    def generator(task_crops=None, max_task_nums=100000):
        counter = 0
        tasks = list()
        for task_crop in task_crops:
            # 这里是使用isinstance类型判断还是使用type属性判断？
            # 个人认为（未测试），属性判断更快
            if task_crop.task_crops_type == TaskCropsType.TaskCrops_Normal:
                return task_crop.command
            elif task_crop.task_crops_type == TaskCropsType.TaskCrops_File:
                input_files = list()
                output_files = list()
                if task_crop.input_files:
                    if len(task_crop.input_files) != task_crop.tasks_num:
                        raise ValueError
                    input_files = task_crop.input_files
                    if task_crop.output_files:
                        if len(task_crop.output_files) == 1:
                            output_files = [task_crop.output_files[0]
                                            for i in range(task_crop.tasks_num)]
                        elif len(task_crop.output_files) == task_crop.tasks_num:
                            output_files = task_crop.output_files
                        else:
                            raise ValueError
                    else:
                        output_dir = join(os.getcwd(), "%s_output"
                                          % (task_crop.task_crops_id))
                        if not isdir(output_dir):
                            os.mkdir(output_dir)
                        output_files = [join(output_dir, "%s.out" % input_file)
                                        for input_file in input_files]
                elif task_crop.input_dir:
                    input_dir = task_crop.input_dir
                    output_dir = task_crop.output_dir
                    if not output_dir:
                        output_dir = join(os.getcwd(), "%s_output"
                                          % (task_crop.task_crops_id))
                    if not isdir(output_dir):
                        os.mkdir(output_dir)
                    input_files = [i for i in os.listdir(input_dir)
                                   if isfile(join(input_dir, i))]
                    if len(input_files) != task_crop.tasks_num:
                        raise ValueError
                    output_files = [join(output_dir, o) for o in input_files]
                    input_files = [join(input_dir, i) for i in input_files]
                else:
                    raise ValueError

                ## 得到确定的输入、输出文件路径后，生成任务信息
                for (input_file, output_file) in zip(input_files, output_files):
                    command = "%s %s >> %s" % (task_crop.exe_program,
                                               input_file, output_file)
                    new_task = Task(job_id="test1",
                                    crops_id=task_crop.task_crops_id,
                                    command=command)
                    tasks.append(new_task)
                    counter += 1
                    if counter == max_task_nums:
                        yield tasks
                        tasks.clear()
                        counter = 0
            else:
                pass



class TaskCropsNormal(TaskCrops):
    '''普通任务团（仅包含一个任务）

    Attributes:
        task_crops_id: 任务团唯一id，可由用户指定也可自动生成（最好自动生成）
        job_id: 任务团所属作业id
        command： 任务执行命令
    '''
    def __init__(self, task_crops_id=None, job_id=None, command=None):

        super(TaskCropsNormal, self).__init__(task_crops_id, job_id)
        self.task_crops_type = TaskCropsType.TaskCrops_Normal
        self.command = command
        self.tasks_num = 1


class TaskCropsFile(TaskCrops):
    '''文件类型任务团

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
    '''
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
    '''参数类型任务团

    Attributes:
        task_crops_id: 任务团唯一id，可由用户指定也可自动生成（最好自动生成）
        job_id: 任务团所属作业id
        exe_program：可执行程序path
        params: 参数列表，形式为[[param1, param2, ...], [], [], ...]
        output_files: 输出文件列表，没有则自动生成
    '''
    def __init__(self, task_crops_id=None, job_id=None, exe_program=None,
                 params=None, output_files=None, params_num=None):
        super(TaskCropsParams, self).__init__(task_crops_id, job_id)
        self.task_crops_type = TaskCropsType.TaskCrops_Para
        self.exe_program = exe_program
        self.params = params
        self.output_files = output_files
        self.tasks_num = params_num


if __name__ == '__main__':
    A = TaskCropsNormal(task_crops_id='A', job_id='test_1')
    B = TaskCropsFile(task_crops_id='B', job_id='test_1')
    C = TaskCropsFile(task_crops_id='C', job_id='test_1')
    D = TaskCropsParams(task_crops_id='D', job_id='test_1')
    A.children.append(B)
    A.children.append(C)
    B.children.append(D)
    C.children.append(D)

    TaskCrops.bfs(A)
    counter1 = 0
    for tasks_info in TaskCrops.generator():
        for task_info in tasks_info:
            print(task_info.task_id, counter1)
            counter1 += 1
