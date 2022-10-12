from redis import Redis
from rq import Queue
from rq.registry import StartedJobRegistry, FinishedJobRegistry

from app.config import Config

redis = Redis(
    host=Config.REDIS_HOST,
    port=Config.REDIS_PORT,
    password=Config.REDIS_PASSWORD,
    db=Config.REDIS_DATABASE,
)

datasets_queue = Queue('dataset', connection=redis)
datasets_started_job_registry = StartedJobRegistry(queue=datasets_queue)
datasets_finished_job_registry = FinishedJobRegistry(queue=datasets_queue)
datasets_failed_job_registry = FinishedJobRegistry(queue=datasets_queue)

