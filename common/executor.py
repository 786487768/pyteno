#!/usr/bin/env python3
import os
import sys
import json
import time
import redis
import psutil
import threading
import multiprocessing

from subprocess import PIPE, Popen

redis_instance = None


def worker(task_queue):
    while 1:
        # time.sleep(1)
        task_info = json.loads(task_queue.get())
        # print(task_info.get('command'))
        args = ["bash", "-c", "-l", task_info.get('command')]
        p = Popen(args, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate()
        print(output)
        task_queue.task_done()
        if task_queue.empty():
            print("worker stop")
            return


class Executor(object):
    """
    任务执行器

    从任务池中按序获取任务执行，并监控计算节点的信息和
    任务执行时的资源使用情况
    """
    def __init__(self, process_num=None, host=None, port=None):
        if not process_num:
            process_num = psutil.cpu_count()
        if not host:
            host = os.getenv("TASK_POOl_HOST")
        if not port:
            port = os.getenv("TASK_POOL_HOST")
        self.process_num = process_num
        self.redis_instance = redis.Redis(host=host, port=port)

    def _producer(self, task_queue):
        while 1:
            if task_queue.qsize() < self.process_num / 2:
                for _ in range(self.process_num * 2):
                    task_info = self.redis_instance.lpop('task_info_list')
                    if task_info is None:
                        return
                    task_queue.put(task_info.decode('UTF-8'))

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

    '''
    def _oversee_resource(self, pids, interval):
        def overseer(pid):
            oversee_process = psutil.Process(pid=pid)
            while 1:
                print(oversee_process.cpu_percent(1))
        threads = []

        while 1:
            pid = pids.get()
            t = threading.Thread(target=overseer, args=(pid,))
            t.start()
            threads.append(t)
            if pids.empty():
                break

        for t in threads:
            t.join()
    '''
    def _oversee_resource(self, process_task_t, interval=1):
        import socket
        hostname = socket.gethostname()
        current_dir = os.getcwd()
        path = os.path.join(current_dir, hostname)
        with open(path, 'a') as resource_info:
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
    process_nums = int(sys.argv[1])
    redis_host = sys.argv[2]
    redis_port = sys.argv[3]
    e = Executor(process_nums, redis_host, redis_port)
    e.run()
