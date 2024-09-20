import signal
import sys
from bumblebee.log import get_logger

logger = get_logger(__name__)

class BigqueryJobHandler:
    def __init__(self) -> None:
        self._sum_slot_millis = 0
        self._sum_total_bytes_processed = 0
        self.client = None
        self.jobs = []
        self._init_signal_handling()

    def _init_signal_handling(self):
        def handle_sigterm(signum, frame):
            self._terminate_jobs()
            sys.exit(1)
        signal.signal(signal.SIGTERM, handle_sigterm)

    def _terminate_jobs(self):
        if self.client and self.jobs:
            for job in self.jobs:
                job_id = job.job_id
                self.client.cancel_job(job_id)
                logger.info(f"{job_id} successfully cancelled")

    def handle_job_finish(self, job) -> None:
        self._sum_slot_millis += job.slot_millis
        self._sum_total_bytes_processed += job.total_bytes_processed

    def register_job(self, client, job):
        self.client = client
        self.jobs.append(job)

    def get_sum_slot_millis(self) -> int:
        return self._sum_slot_millis

    def get_sum_total_bytes_processed(self) -> int:
        return self._sum_total_bytes_processed
