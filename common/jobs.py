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
    def

