import signal
import sys
from bumblebee.log import get_logger
import threading
import time

logger = get_logger(__name__)

class BigqueryJobHandler:
    def __init__(self) -> None:
        self._sum_slot_millis = 0
        self._sum_total_bytes_processed = 0
        self.client = None
        self.jobs = []
        self.is_done = False
        self.is_cancel = False
        self._thread = threading.Thread(target=self._handle_state)
        self._thread.start()
        self._init_signal_handling()

    def _init_signal_handling(self):
        def handle_sigterm(signum, frame):
            self.is_cancel = True
            self._thread.join()
            sys.exit(1)
        signal.signal(signal.SIGTERM, handle_sigterm)

    def _handle_state(self):
        while not self.is_cancel and not self.is_done:
            time.sleep(1)
        if self.is_cancel:
            self._terminate_jobs()

    def _terminate_jobs(self):
        if self.client and self.jobs:
            for job in self.jobs:
                job_id = job.job_id
                self.client.cancel_job(job_id)
                logger.info(f"{job_id} successfully cancelled")

    def handle_job_finish(self, job) -> None:
        self._sum_slot_millis += job.slot_millis
        self._sum_total_bytes_processed += job.total_bytes_processed
        self.is_done = True

    def handle_job_cancelled(self, client, job):
        self.client = client
        self.jobs.append(job)

    def get_sum_slot_millis(self) -> int:
        return self._sum_slot_millis

    def get_sum_total_bytes_processed(self) -> int:
        return self._sum_total_bytes_processed
