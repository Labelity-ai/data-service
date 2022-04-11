from typing import List, Union
from datetime import datetime
from rq.decorators import job
from fastapi import HTTPException
import aioboto3
from odmantic import query
import json

from app.schema import PipelinePostData, PipelinePatchData
from app.models import Pipeline, ObjectId, PipelineRun, engine, RunStatus
from app.services.annotations import AnnotationsService
from app.core.queue import redis
from app.models import Project
from app.config import Config
from app.core.tracing import traced

session = aioboto3.Session()


def generate_pipeline_run_logs_s3_key(project: Project, run: PipelineRun):
    return f'{Config.PIPELINES_LOGS_FOLDER}/{project.id}/{run.pipeline_id}/{run.id}'


def generate_pipeline_run_results_s3_key(project: Project, run: PipelineRun):
    return f'{Config.PIPELINES_RESULTS_FOLDER}/{project.id}/{run.pipeline_id}/{run.id}'


@job(queue='pipelines', connection=redis)
async def _run_pipeline(pipeline: Pipeline, project: Project):
    results = await AnnotationsService.run_annotations_pipeline(query=pipeline.nodes, project=project)
    run = await engine.find_one(PipelineRun, PipelineRun.pipeline_id == pipeline.id)
    run.finished_at = datetime.now()
    await engine.save(run)

    async with session.client('s3') as s3_client:
        data = [x.dict() for x in results.data]
        await s3_client.put_object(
            Body=json.dumps(data),
            Bucket=Config.PIPELINES_BUCKET,
            Key=generate_pipeline_run_results_s3_key(project, run))


@traced
class PipelinesService:
    @staticmethod
    async def create_pipeline(pipeline: PipelinePostData, project: Project) -> Pipeline:
        pipeline_instance = Pipeline(**pipeline.dict(), project_id=project.id)
        pipeline_instance = await engine.save(pipeline_instance)
        return await engine.save(pipeline_instance)

    @staticmethod
    async def get_pipelines(project: Project) -> List[Pipeline]:
        pipeline = await engine.find(Pipeline, Pipeline.project_id == project.id)
        if not pipeline:
            raise HTTPException(404)
        return pipeline

    @staticmethod
    async def get_pipeline(id: ObjectId) -> List[Pipeline]:
        return await engine.find_one(Pipeline, Pipeline.id == id)

    @staticmethod
    async def delete_pipeline(pipeline: Pipeline):
        pipeline.deleted = True
        engine.save(pipeline)

    @staticmethod
    async def update_pipeline(pipeline: Pipeline, data: Union[PipelinePatchData, PipelinePostData]) -> Pipeline:
        if data.name is not None:
            pipeline.name = data.name
        if data.nodes is not None:
            pipeline.nodes = data.nodes
        if data.description is not None:
            pipeline.description = data.description
        if data.tags is not None:
            pipeline.tags = data.tags

        return await engine.save(pipeline)

    @staticmethod
    async def run_pipeline(pipeline: Pipeline, project: Project) -> PipelineRun:
        job = _run_pipeline.delay(pipeline, project)
        run = PipelineRun(
            pipeline_id=pipeline.id,
            job_id=job.id,
            status=RunStatus.IN_PROGRESS,
            started_at=datetime.now(),
            finished_at=None,
            scheduled_by=None
        )
        return await engine.save(run)

    @staticmethod
    async def get_pipeline_runs(pipeline: Pipeline):
        return await engine.find(
            PipelineRun,
            PipelineRun.pipeline_id == pipeline.id,
            sort=query.desc(PipelineRun.finished_at),
        )

    @staticmethod
    async def get_pipeline_run(pipeline_run_id: ObjectId) -> PipelineRun:
        run = await engine.find_one(PipelineRun, PipelineRun.id == pipeline_run_id)
        if not run:
            raise HTTPException(404)
        return run

    @staticmethod
    async def get_pipeline_run_logs(run: PipelineRun, project: Project) -> str:
        async with session.client("s3") as s3_client:
            key = generate_pipeline_run_logs_s3_key(project, run)
            data = s3_client.get_object(Bucket=Config.PIPELINES_BUCKET, Key=key)
            contents = data['Body'].read()
            return contents.decode("utf-8")
