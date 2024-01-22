from psij import Job, JobSpec, JobExecutor
from pathlib import Path
import time

ex = JobExecutor.get_instance("nqsv")
job = Job(
    JobSpec(
        executable="/home/gp.sc.cc.tohoku.ac.jp/tanizawa/PSI_J/project/run.sh",
        stdout_path=Path("/home/gp.sc.cc.tohoku.ac.jp/tanizawa/PSI_J/project/result/stdout.txt"),
        stderr_path=Path("/home/gp.sc.cc.tohoku.ac.jp/tanizawa/PSI_J/project/result/stderr.txt") 
    )
)


ex.submit(job)
ex.cancel(job)
while(True):
    print(job.status)
    time.sleep(0.1)
