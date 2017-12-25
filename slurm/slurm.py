# call Slurm to execute tasks and return result or error
import os 
import json
import configparser
from subprocess import Popen, PIPE

class Slurm(object):
    """Manage slurm conf and execute tasks"""
    def __init__(self, slurm_exec, slurm_argc=None):
        super(Slurm, self).__init__()
        self.slurm_exec = slurm_exec
        self.slurm_argc = json.loads(slurm_argc)
        self.cmd_path = {}
        self._parse_config()

    def run_task(self):
        " Run task using slurm "
        slurm_cmd = [self.cmd_path[self.slurm_exec]]
        if self.slurm_argc:
            slurm_cmd.extend(self.slurm_argc)
        p = Popen(slurm_cmd, stdout=PIPE, stderr=PIPE)
        (result, error) = p.communicate()
        return (result, error)

    def test_run(self):
        " Run task using slurm "
        slurm_cmd = [self.cmd_path[self.slurm_exec]]
        if self.slurm_argc:
            slurm_cmd.extend(self.slurm_argc)
        p = Popen(slurm_cmd, stdout=PIPE, stderr=PIPE)

    def _parse_config(self):
        " Parse configure file to get the path of slurm cmd "
        conf_section = 'PATH'
        conf_slurm_path = 'slurm_path'
        conf_srun = 'srun'
        conf_sbatch = 'sbatch'
        conf_salloc = 'salloc'
        conf_scancel = 'scancel'
        conf_scontrol = 'scontrol'
        conf_openmpi_path = 'openmpi_path'
        conf_mpi_path = 'mpi_path'

        conf_path = os.getenv('TENO_SLURM_CONF')
        if not conf_path:
            conf_path = "./teno-slurm.conf"

        config = configparser.ConfigParser()
        config.read(conf_path)
        if conf_path in config.sections():
            section = config[conf_section]
            self.slurm_path = section.get(conf_slurm_path)
            self.cmd_path[conf_srun] = section.get(conf_srun)
            self.cmd_path[conf_sbatch] = section.get(conf_sbatch)
            self.cmd_path[conf_salloc] = section.get(conf_salloc)
            self.cmd_path[conf_scancel] = section.get(conf_scancel)
            self.cmd_path[conf_scontrol] = section.get(conf_scontrol)
            self.cmd_path[conf_openmpi_path] = section.get(conf_openmpi_path)
            self.cmd_path[conf_mpi_path] = section.get(conf_mpi_path)
        self._check_conf()

    def _check_conf(self):
        " Checking slurm conf is configured, if not assign defalut value"
        conf_slurm_path = 'slurm_path'
        conf_srun = 'srun'
        conf_sbatch = 'sbatch'
        conf_salloc = 'salloc'
        conf_scancel = 'scancel'
        conf_scontrol = 'scontrol'
        default_slurm_path = "/usr"
        default_srun = "/usr/bin/srun"
        default_sbatch = "/usr/bin/sbatch"
        default_salloc = "/usr/bin/salloc"
        default_scancel = "/usr/bin/scancel"
        default_scontrol = "/usr/bin/scontrol"
        if not self.cmd_path.get(conf_slurm_path):
            self.cmd_path[conf_slurm_path] = default_slurm_path
        if not self.cmd_path.get(conf_srun):
            self.cmd_path[conf_srun] = default_srun
        if not self.cmd_path.get(conf_sbatch):
            self.cmd_path[conf_sbatch] = default_sbatch
        if not self.cmd_path.get(conf_salloc):
            self.cmd_path[conf_salloc] = default_salloc
        if not self.cmd_path.get(conf_scancel):
            self.cmd_path[conf_scancel] = default_scancel
        if not self.cmd_path.get(conf_scontrol):
            self.cmd_path[conf_scontrol] = default_scontrol


def run(task_exec, slurm_argc):
    slurm_task = Slurm(task_exec, slurm_argc)
    (result, error) = slurm_task.run_task()
    return_code = 0 if result else 1
    return (return_code, result.decode('UTF-8'), error.decode('UTF-8'))

if __name__ == '__main__':
    slurm_task = Slurm('srun', ['hostname'])
    result, error = slurm_task.run_task()
    return_code = 0 if result else 1
    print (return_code, result.decode('UTF-8'), error.decode('UTF-8'))