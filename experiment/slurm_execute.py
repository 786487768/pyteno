#!/usr/bin/env python3
import os
import sys
import json
import time
import psutil
import threading
import multiprocessing

from subprocess import PIPE, Popen

def worker(task_queue):
    import socket
    hostname = socket.gethostname()
    current_pid = str(os.getpid())
    task_time_dir = os.path.join(os.getcwd(), 'task_time')
    if not os.path.isdir(task_time_dir):
        os.mkdir(task_time_dir)
    task_time_path = os.path.join(task_time_dir, "%s_%s" % (hostname, current_pid))
    task_time_list = []
    while 1:
        try:
            task_info = task_queue.get(timeout=3)
        except Exception:
            with open(task_time_path, 'w+') as f:
                f.write('\n'.join(task_time_list))
                f.write('\n')
            return
        # print(task_info.get('command'))
        command = task_info.get('command')
        start_time = task_info.get('start_time')
        args = ["bash", "-c", "-l", command]
        p = Popen(args, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate()
        return_code = p.poll()
        print(output)
        end_time = time.time()
        spend_time = str(end_time-start_time)
        task_time_list.append(spend_time)
        task_queue.task_done()
        if task_queue.empty():
            print("worker stop")
            with open(task_time_path, 'w+') as f:
                f.write('\n'.join(task_time_list))
                f.write('\n')
            return


class Executor(object):
    """
    任务执行器

    从任务池中按序获取任务执行，并监控计算节点的信息和
    任务执行时的资源使用情况
    """
    def __init__(self, process_num=None, input_file='htc.input', node_index=0):
        if not process_num:
            process_num = psutil.cpu_count()
        self.process_num = process_num
        self.input_file =input_file
        self.node_index = node_index

    def _producer(self, task_queue):
        with open(self.input_file) as f:
            tasks = json.loads(f.readline())[self.node_index]
            for task in tasks:
                print(task)
                task_queue.put(task)

    def _process_task(self):
        task_queue = multiprocessing.Manager().Queue()
        producer = multiprocessing.Process(target=self._producer, args=(task_queue,))

        p = multiprocessing.Pool(self.process_num)
        for i in range(self.process_num):
            p.apply_async(worker, (task_queue,))

        producer.start()
        producer.join()
        p.close()
        p.join()

        print("worker over")
        return

    def _oversee_resource(self, process_task_t, interval=1):
        import socket
        hostname = socket.gethostname()
        current_dir = os.getcwd()
        resource_info_dir = os.path.join(current_dir, 'resource_info_dir')
        if not os.path.isdir(resource_info_dir):
            os.mkdir(resource_info_dir)
        path = os.path.join(resource_info_dir, hostname)
        with open(path, 'w+') as resource_info:
            cpu_percents = []
            timer = 0
            while 1:
                cpu_percents.append(str(psutil.cpu_percent()))
                timer += 1
                print(process_task_t.isAlive())
                if not process_task_t.isAlive() or timer == 10:
                    resource_info.write('\n'.join(cpu_percents))
                    resource_info.write('\n')
                    cpu_percents.clear()
                    timer = 0
                    if not process_task_t.isAlive():
                        return
                time.sleep(interval)

    def run(self, interval=1):
        process_task_t = threading.Thread(target=self._process_task,
                                          name="ProcessTaskThread")
        oversee_t = threading.Thread(target=self._oversee_resource,
                                     args=(process_task_t, interval,), name="OverseeThread")

        process_task_t.start()
        oversee_t.start()
        process_task_t.join()
        oversee_t.join()


if __name__ == '__main__':
    import socket
    process_nums = int(sys.argv[1])
    input_file = sys.argv[2]
    master_name = sys.argv[3]
    #s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # 建立连接:
    #s.connect((master_name, 16181))
    # 接收欢迎消息:
    # node_index = int(s.recv(1024).decode('utf-8'))
    # s.close()
    node_index = 0
    e = Executor(process_nums, input_file, node_index)
    e.run()
