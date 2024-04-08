from pathlib import Path
from psij import Job, JobState, JobStatus
from typing import Optional, List, Dict, TextIO, Collection 
from psij.executors.batch.script_generator import TemplatedScriptGenerator
from psij.executors.batch.batch_scheduler_executor import BatchSchedulerExecutor, BatchSchedulerExecutorConfig, check_status_exit_code

import re
import subprocess



class NQSVExecutorConfig(BatchSchedulerExecutorConfig):
    pass

class NQSVJobExecutor(BatchSchedulerExecutor):
    
    _STATE_MAP = {
        'QUE': JobState.QUEUED,
        'RUN': JobState.ACTIVE,
        'WAT': JobState.QUEUED,
        'HLD': JobState.QUEUED,
        'SUS': JobState.QUEUED,
        'ARI': JobState.QUEUED, #転送キューからの受信中
        'TRS': JobState.QUEUED, #転送キューからの送信中
        'EXT': JobState.COMPLETED, #実行結果ファイルの転送中
        'PRR': JobState.QUEUED, #実行前処理中
        'POR': JobState.COMPLETED, #実行後処理中
        'MIG': JobState.QUEUED, #動的ジョブマイグレーションによるリクエスト移動中
        'STG': JobState.QUEUED,  #ジョブの生成中、およびステージイン対象ファイルの実行ホスト上への転送中
    }


    def __init__(self, url: Optional[str] = None, config: Optional[NQSVExecutorConfig] = None):
        if config is None:
            config = BatchSchedulerExecutorConfig()
        super().__init__(url=url, config=config)
        self.generator = TemplatedScriptGenerator(config, Path(__file__).parent / 'nqsv.mustache')
        self.submit_frag = False
        self.cancel_frag = False

    def generate_submit_script(self, job: Job, context: Dict[str, object], submit_file: TextIO) -> None:
        self.generator.generate_submit_script(job, context, submit_file)

    def get_submit_command(self, job: Job, submit_file_path: Path) -> List[str]:
        return ['qsub', str(submit_file_path.absolute())]

    def job_id_from_submit_output(self, out: str) -> str:
        self.submit_frag = True 
        s = out.strip().split()[1]
        out = ""
        for char in s:
            if char.isdigit():
                out += char
        return out #ここでnative_idがずれていたのが原因

    def get_cancel_command(self, native_id: str)-> List[str]:
        self.cancel_frag = True #qdelの完了を確認してからcancel_fragを立てるのはExecutorクラス内では無理なのでは？
        return ['qdel', native_id]

    def process_cancel_command_output(*args, **kwargs):
        raise NotImplementedError

    def get_status_command(self, native_ids: Collection[str]) -> List[str]:
        return ['qstat', '-F', 'rid,stt', '-n', '-l'] + list(native_ids) 

    def parse_status_output(self, exit_code: int, out: str) -> Dict[str, JobStatus]:
        check_status_exit_code('qstat', exit_code, out)
        r = {}
        lines = iter(out.split('\n'))
        for line in lines:
            if not line:
                continue
            
            cols = line.split()

            if(len(cols) == 8 and self.cancel_frag):
                s = cols[2]
                native_id = ""
                for char in s:
                    if char.isdigit():
                        native_id += char
                state = JobState.CANCELED
                r[native_id] = JobStatus(state=state, message=None)
                return r
            
            elif(len(cols) == 8):
                s = cols[1]
                native_id = ""
                for char in s:
                    if char.isdigit():
                        native_id += char
                state = JobState.COMPLETED
                r[native_id] = JobStatus(state=state, message=None)
                return r
            
            else:
                assert len(cols) == 2
                match = re.search(r'\b(\d+)\b', cols[0])
                native_id = match.group(1) if match else None
                native_state = cols[1]
                state = self._get_state(native_state) ##stateにはAVTIVEが入っている
                msg = None
                r[native_id] = JobStatus(state=state, message=msg)

                return r 
                
######入力されたjobクラスのJob.Statusを出力する######
    def get_status_now(self, job: Job) -> Job.status:
        native_ids = ''.join(str(job.native_id))
        command = ['qstat', '-F', 'rid,stt', '-n', '-l', native_ids]
        out = subprocess.run(command, capture_output=True, text=True).stdout

        r = {}
        lines = iter(out.split('\n'))
        for line in lines:
            if not line:
                continue
            cols = line.split()

            if(len(cols) == 8 and self.cancel_frag):
                s = cols[2]
                native_id = ""
                for char in s:
                    if char.isdigit():
                        native_id += char
                state = JobState.CANCELED
                r[native_id] = JobStatus(state=state, message=None)
                return r[native_id]
            
            elif(len(cols) == 8 and not(self.cancel_frag)):
                s = cols[2]
                native_id = ""
                for char in s:
                    if char.isdigit():
                        native_id += char
                state = JobState.COMPLETED
                r[native_id] = JobStatus(state=state, message=None)
                return r[native_id]
            
            elif(not(self.submit_frag)):
                s = cols[2]
                native_id = ""
                for char in s:
                    if char.isdigit():
                        native_id += char
                state = JobState.FAILED
                r[native_id] = JobStatus(state=state, message=None)
                return r[native_id]
            
            else:
                assert len(cols) == 2
                match = re.search(r'\b(\d+)\b', cols[0])
                native_id = match.group(1) if match else None
                native_state = cols[1]
                state = self._get_state(native_state) ##stateにはAVTIVEが入っている
                msg = None
                r[native_id] = JobStatus(state=state, message=msg)
                return r[native_id]

    ### 未実装
    def _get_messsage(*args, **kwargs): #ToDo qstat -f の下2行目からメッセージを取得する関数 
        return None
    
    def _get_state(self, state: str) -> JobState:  #_STATE_MAPからジョブのstateを取得する  
        assert state in NQSVJobExecutor._STATE_MAP
        return NQSVJobExecutor._STATE_MAP[state]
    
    def get_list_command(*args, **kwargs):
        raise NotImplementedError
    
    def get_hold_command(self, native_id: str) -> List[str]:
        return ['qhold', native_id]
    
    def get_release_command(self, native_id: str) -> List[str]:
        return ['qrls', native_id]
