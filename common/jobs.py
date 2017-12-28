import os
import uuid
import queue

from os.path import isfile, isdir, join
from common.tasks import Task, TaskCropsType
from slurm.slurm import Slurm

class JobState(object):
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

class Job(object):

    def __init__(self, job_id=None, job_name=None, job_describe=None):
        if job_id:
            self.job_id = job_id
        else:
            self.job_id = str(uuid.uuid1()).split('-')[0]
        self.root_task_crops = None
        self.job_name = job_name
        self.job_describe = job_describe

    def set_root_task_crops(self, root):
        if not isinstance(root, list):
            raise TypeError("root task crops must be list")
        self.root_task_crops = root

    def bfs(self):
        task_crops = list()
        tag = dict()
        task_crops_queue = queue.Queue()
        root_task_crops = self.root_task_crops[0]
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
                child_task_crops.parents.append(current_task_crops.task_crops_id)
                if child_task_crops.task_crops_id not in tag:
                    task_crops_queue.put(child_task_crops)
                    tag[child_task_crops.task_crops_id] = 1
        return task_crops

    def generator(self, task_crops=None, max_task_nums=10):
        counter = 0
        tasks = list()
        for task_crop in task_crops:
            # 这里是使用isinstance类型判断还是使用type属性判断？
            # 私以为（未测试），属性判断更快
            if task_crop.task_crops_type == TaskCropsType.TaskCrops_Normal:
                for _ in range(task_crop.tasks_num):
                    new_task = Task(job_id=self.job_id,
                                    crops_id=task_crop.task_crops_id,
                                    dependence=task_crop.parents,
                                    command=task_crop.command)
                    tasks.append(new_task)
                    counter += 1
                    if counter == max_task_nums:
                        yield tasks
                        tasks.clear()
                        counter = 0
            elif task_crop.task_crops_type == TaskCropsType.TaskCrops_File:
                # input_files = list()
                # output_files = list()
                if task_crop.input_files:
                    if len(task_crop.input_files) != task_crop.tasks_num:
                        raise ValueError
                    input_files = task_crop.input_files
                    if task_crop.output_files:
                        if len(task_crop.output_files) == 1:
                            output_files = [task_crop.output_files[0]
                                            for _ in range(task_crop.tasks_num)]
                        elif len(task_crop.output_files) == task_crop.tasks_num:
                            output_files = task_crop.output_files
                        else:
                            raise ValueError
                    else:
                        output_dir = join(os.getcwd(), "{id}_output".format(
                            id=task_crop.task_crops_id))
                        if not isdir(output_dir):
                            os.mkdir(output_dir)
                        output_files = [join(output_dir, "%s.out" % input_file)
                                        for input_file in input_files]
                elif task_crop.input_dir:
                    input_dir = task_crop.input_dir
                    output_dir = task_crop.output_dir
                    if not output_dir:
                        output_dir = join(os.getcwd(), "{id}_output".format(
                            id=task_crop.task_crops_id))
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

                # 得到确定的输入、输出文件路径后，生成任务信息
                for (input_file, output_file) in zip(input_files, output_files):
                    command = "%s %s >> %s" % (task_crop.exe_program,
                                               input_file, output_file)
                    new_task = Task(job_id=self.job_id,
                                    crops_id=task_crop.task_crops_id,
                                    dependence=task_crop.parents,
                                    command=command)
                    tasks.append(new_task)
                    counter += 1
                    if counter == max_task_nums:
                        yield tasks
                        tasks.clear()
                        counter = 0
            # 参数类型任务团
            elif task_crop.task_crops_type == TaskCropsType.TaskCrops_Para:
                param_nums = len(task_crop.exe_program_params)
                for index in task_crop.replace_param_index:
                    if index >= param_nums:
                        print("replace index out of range")
                        raise ValueError
                task_nums = len(task_crop.replace_params[0])
                for replace_params in task_crop.replace_params:
                    if task_nums != len(replace_params):
                        print("replace_params num error")
                        raise ValueError
                for replace_num in range(task_nums):
                    command = task_crop.exe_program_params
                    index_counter = 0
                    for replace_param_index in task_crop.replace_param_index:
                        command[replace_param_index] = task_crop.replace_params[index_counter][replace_num]
                        index_counter += 1
                    command_string = ' '.join(command)
                    new_task = Task(job_id=self.job_id,
                                    crops_id=task_crop.task_crops_id,
                                    dependence=task_crop.parents,
                                    command=command_string)
                    tasks.append(new_task)
                    counter += 1
                    if counter == max_task_nums:
                        yield tasks
                        tasks.clear()
                        counter = 0
            else:
                pass
        yield tasks

    def submitter(self, task_crops=None, thread_nums=5, host='localhost', port=6379):
        import redis
        import threading

        def worker():
            while 1:
                task_info = task_queue.get()
                if task_info is None:
                    break
                print(task_info.to_json_string())
                redis_instance.lpush('task_info_list', task_info.to_json_string())
                task_queue.task_done()

        task_queue = queue.Queue()
        redis_instance = redis.Redis(host=host, port=port)
        threads = []
        for _ in range(thread_nums):
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)
        for tasks in self.generator(task_crops=task_crops):
            for task in tasks:
                task_queue.put(task)
            task_queue.join()

        # 停止consumer
        for i in range(thread_nums):
            task_queue.put(None)
        for t in threads:
            t.join()

    def run(self, redis_hostname='localhost', redis_port=6379):
        if self.root_task_crops.count == 0:
            print("Please set root task crops")
            return
        task_crops_info = self.bfs()
        print(task_crops_info)

        self.submitter(task_crops_info, host=redis_hostname, port=redis_port)


if __name__ == '__main__':
    """A = TaskCropsNormal(task_crops_id='A', job_id='test_1')
    B = TaskCropsFile(task_crops_id='B', job_id='test_1',
                      input_files=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], files_num=10)
    C = TaskCropsFile(task_crops_id='C', job_id='test_1',
                      input_files=['11', '12', '13', '14', '15', '16', '17', '18', '19', '20'], files_num=10)
    D = TaskCropsNormal(task_crops_id='D', job_id='test_1')
    A.children.append(B)
    A.children.append(C)
    B.children.append(D)
    C.children.append(D)"""

    import sys
    from common.tasks import TaskCropsNormal, TaskCrops
    redis_hostname = sys.argv[1]
    redis_port = sys.argv[2]
    A = TaskCropsNormal(task_crops_id='A', job_id='test_1',
                        command='/home/ll/PycharmProjects/cal_time 1', tasks_num=20)
    cal_time = Job()
    cal_time.set_root_task_crops([A])
    cal_time.run(redis_hostname, redis_port)



