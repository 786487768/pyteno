import uuid


class TaskCropsType():
    TaskCrops_Normal = 0
    TaskCrops_File = 1
    TaskCrops_Para = 2
    TaskCrops_Other = -1


class TaskSTate():
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
    def __init__(self, task_id, job_id, crops_id, command):
        if not task_id:
            self.task_id = uuid.uuid1()
        else:
            self.task_id = task_id
        if not job_id:
            print("job id is none")
        else:
            self.job_id = job_id
        if not crops_id:
            print("crops id is none")
        else:
            self.crops_id = crops_id
        self.command = command
        self.state = TaskSTate.SUBMITTED
        self.start_time = None
        self.end_time = None
        self.error_info = None


if __name__ == '__main__':
    new_task = Task()
    print(new_task.name)
