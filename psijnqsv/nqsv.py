from pathlib import Path
from psij import Job, JobState, JobStatus
from typing import Optional, List, Dict, TextIO, Collection 
from psij.executors.batch.script_generator import TemplatedScriptGenerator
from psij.executors.batch.batch_scheduler_executor import BatchSchedulerExecutor, BatchSchedulerExecutorConfig, check_status_exit_code




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
        'EXT': JobState.ACTIVE, #実行結果ファイルの転送中
        'PRR': JobState.QUEUED, #実行前処理中
        'POR': JobState.QUEUED, #実行後処理中
        'MIG': JobState.QUEUED, #動的ジョブマイグレーションによるリクエスト移動中
        'STG': JobState.QUEUED  #ジョブの生成中、およびステージイン対象ファイルの実行ホスト上への転送中
    }


    def __init__(self, url: Optional[str] = None, config: Optional[NQSVExecutorConfig] = None):
        if config is None:
            config = BatchSchedulerExecutorConfig()
        super().__init__(url=url, config=config)
        self.generator = TemplatedScriptGenerator(config, Path(__file__).parent / 'nqsv.mustache')
        self.submit_frag = False
        self.cancel_frag = False
        print(1)

    def generate_submit_script(self, job: Job, context: Dict[str, object], submit_file: TextIO) -> None:
        self.generator.generate_submit_script(job, context, submit_file)
        print(2)

    def get_submit_command(self, job: Job, submit_file_path: Path) -> List[str]:
        
        print(3)
        return ['qsub', str(submit_file_path.absolute())]

    def job_id_from_submit_output(self, out: str) -> str:
        print(4)
        self.submit_frag = True 
        return out.strip().split()[1] #ここでnative_idがずれていたのが原因

    def get_cancel_command(self, native_id: str)-> List[str]:
        self.cancel_frag = True #qdelの完了を確認してからcancel_fragを立てるのは無理なのでは？
        print(5)
        return ['qdel', native_id]

    def process_cancel_command_output(*args, **kwargs):
        print(6)
        raise NotImplementedError
    


    def get_status_command(self, native_ids: Collection[str]) -> List[str]:
        print(7)
        # ids = [item.split()[1] for item in list(native_ids)]    #list(native_ids)の中身がRequest 98.tokyo.sc.cc.tohoku.ac.jp submitted to queue: execque1.であったためjobIDだけ抽出
        return ['qstat', '-F', 'rid,stt', '-n', '-l'] + list(native_ids)      #ids = 98.tokyo.sc.cc.tohoku.ac.jp

    def parse_status_output(self, exit_code: int, out: str) -> Dict[str, JobStatus]:
        
        print(8)

        check_status_exit_code('qstat', exit_code, out)
        r = {}
        lines = iter(out.split('\n'))
        
        for line in lines:
                
                if not line:
                    continue
                cols = line.split()
                if(len(cols) == 8 and self.cancel_frag):
                    native_id = cols[2]
                    state = JobState.CANCELED
                    r[native_id] = JobStatus(state=state, message=None)
                    return r
                
                assert len(cols) == 2
                native_id = cols[0]
                native_state = cols[1]
                state = self._get_state(native_state) ##stateにはAVTIVEが入っている
                msg = None
                r[native_id] = JobStatus(state=state, message=msg)
        
                return r 

        
        # cols = out.split()

        # if len(cols) == 2:
        #     native_id = cols[0]
        #     state = self._get_state(cols[1])
        #     message = self._get_messsage    #NQSVにメッセージに該当する箇所 qstat -f の下から2行目

        #     r[native_id] = JobStatus(state, message=message)

        # else:
        #     native_id = cols[2]
        #     message = self._get_messsage
        #     state = JobState.CANCELED

        #     # if self.submit_frag and self.cancel_frag:
        #     #     state = JobState.CANCELED
        #     # elif self.submit_frag and not(self.cancel_frag):
        #     #     state = JobState.COMPLETED
        #     # else:
        #     #     state = JobState.FAILED

        #     r[native_id] = JobStatus(state, message=message)
            
        # return r
    
    ### 未実装
    def _get_messsage(*args, **kwargs): #ToDo qstat -f の下2行目からメッセージを取得する関数 
        print(9)
        return None
    
    def _get_state(self, state: str) -> JobState:  #_STATE_MAPからジョブのstateを取得する  
        print(10)
        assert state in NQSVJobExecutor._STATE_MAP
        return NQSVJobExecutor._STATE_MAP[state]
    
    def get_list_command(*args, **kwargs):
        print(11)
        raise NotImplementedError