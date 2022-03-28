import logging
import time
import json
from multiprocessing.pool import ThreadPool
from threading import Thread, Event, Lock

class Orchestrator:
    def __init__(self, n_workers=4, logging_interval=10):
        self.pool = ThreadPool(processes=n_workers)
        self.queued = {}
        self.active = {}
        self.thread = Thread(target=self.__start)
        self.lock = Lock()
        self.__stop_event = Event()
        self.last_logged = 0
        self.logging_interval=logging_interval
        self.thread.start()

    def __start(self):
        logging.debug("Orchestrator started")
        while not self.__stop_event.is_set():
            finished_jobs = []
            # Check for job completion
            for job_name, worker in self.active.items():
                if worker.ready():
                    finished_jobs.append(job_name)
                    if worker.successful():
                        logging.debug(f"{job_name} finished")
                    else:
                        try:
                            worker.get()
                        except Exception as e:
                            logging.error(f"{job_name} failed, dumping error\n{e}")
            while len(finished_jobs) > 0:
                self.active.pop(finished_jobs.pop())
            # Submit jobs that do not have the same job name as any active job
            self.lock.acquire()
            for job_name, job_queue in self.queued.items():
                if job_name not in self.active.keys():
                    worker_func, job_args = job_queue.pop()
                    self.active[job_name] = self.pool.apply_async(worker_func, job_args)
                    logging.debug(f"{job_name} submitted")
                    if len(job_queue) == 0:
                        finished_jobs.append(job_name)
            while len(finished_jobs) > 0:
                self.queued.pop(finished_jobs.pop())
            # Logging
            now = time.time()
            if (now - self.last_logged) >= self.logging_interval:
                if self.active:
                    logging.debug(f"Active jobs: {', '.join(self.active)}")
                    queue_lengths = [f"{n}: {len(q)}" for n, q in self.queued.items()]
                    logging.debug(f"Queued jobs: {', '.join(queue_lengths)}")
                else:
                    logging.debug(f"No active orchestrator jobs")
                self.last_logged = now
            self.lock.release()

    def stop(self):
        self.pool.close()
        self.pool.terminate()
        self.__stop_event.set()
        self.thread.join()

    def put(self, job_name, worker_func, job_args):
        self.lock.acquire()
        if job_name in self.queued.keys():
            self.queued[job_name].insert(0, (worker_func, job_args))
        else:
            self.queued[job_name] = [(worker_func, job_args)]
        self.lock.release()
