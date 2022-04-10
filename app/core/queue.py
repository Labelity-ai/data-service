from redis import Redis
from rq import Queue
from rq.registry import StartedJobRegistry, FinishedJobRegistry, FailedJobRegistry

from app.config import Config

redis = Redis.from_url(Config.REDIS_URI)
datasets_queue = Queue('dataset', connection=redis)
datasets_started_job_registry = StartedJobRegistry(queue=datasets_queue)
datasets_finished_job_registry = FinishedJobRegistry(queue=datasets_queue)
datasets_failed_job_registry = FinishedJobRegistry(queue=datasets_queue)

