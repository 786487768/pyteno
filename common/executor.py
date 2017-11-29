#!/usr/bin/env python
import os
import json
import redis
import psutil
import threading

from multiprocessing import Pool

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


	def _process_task(self):
		def worker(a):
			import time
			i = 0
			print(a, os.getpid())
			#while 1:
				#time.sleep(1)
				#task_info = self.redis_instance.lpop('task_info_list')
				#if task_info is None:
				#	break
				#print(task_info)
			#	i += 1
				#print(os.getpid(), a)

		p = Pool(self.process_num)
		for i in range(self.process_num):
			p.apply_async(worker(i))

		p.close()
		p.join()

	def _oversee_resource(self):
		while 1:
			print(psutil.cpu_percent(1))


	def run(self):
		oversee_t = threading.Thread(target=self._oversee_resource, name="OverseeThread")
		process_task_t = threading.Thread(target=self._process_task, name="ProcessTaskThread")

		process_task_t.start()
		oversee_t.start()
		process_task_t.join()
		oversee_t.join()

if __name__ == '__main__':
	e = Executor(4, 'localhost', 6379)
	e.run()






