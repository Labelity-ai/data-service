from redis import Redis
from rq import Queue
from rq.registry import StartedJobRegistry, FinishedJobRegistry, FailedJobRegistry

redis = Redis()
datasets_queue = Queue('dataset', connection=redis)
datasets_started_job_registry = StartedJobRegistry(queue=datasets_queue)
datasets_finished_job_registry = FinishedJobRegistry(queue=datasets_queue)
datasets_failed_job_registry = FinishedJobRegistry(queue=datasets_queue)

