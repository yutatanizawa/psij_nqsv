from psij import Job, JobSpec, JobExecutor
from pathlib import Path

ex = JobExecutor.get_instance("slurm")
job = Job(
    JobSpec(
        executable="/home/gp.sc.cc.tohoku.ac.jp/tanizawa/PSI_J/project/run.sh",
        stdout_path=Path("/home/gp.sc.cc.tohoku.ac.jp/tanizawa/PSI_J/project/result/stdout.txt"),
        stderr_path=Path("/home/gp.sc.cc.tohoku.ac.jp/tanizawa/PSI_J/project/result/stderr.txt") 
    )
)

job._native_id = 10856 #job番号を指定してjobインスタンスを作成
ex.release(job)