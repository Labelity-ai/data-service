from typing import List, Dict, Union

from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends, HTTPException, status, File, UploadFile

from app.schema import PipelineRunInfo, PipelinePostData
from app.models import ImageAnnotations, Project
from app.security import get_project
from app.config import Config
from app.services.pipelines import PipelinesService

router = InferringRouter(
    tags=["pipeines"],
)


@cbv(router)
class PipelinesView:
    project: Project = Depends(get_project)

    @router.get("/pipelines/")
    async def get_pipelines(self, page: int = 0, page_size: int = 10):
        return await PipelinesService.get_pipelines(limit=page_size, page=page, project_id=self.project.id)

    @router.get("/pipelines/{id}")
    async def get_pipeline(self, id):
        return await PipelinesService.get_pipeline(id=id)

    @router.delete("/pipelines/{id}")
    async def delete_pipeline(self, id):
        pipeline = await self.get_pipeline(id)
        return await PipelinesService.remove_pipeline(pipeline)

    @router.get("/pipelines/{id}/runs")
    async def get_pipeline_runs(self, id, page: int = 0, page_size: int = 10):
        pipeline = await self.get_pipeline(id)
        return await PipelinesService.get_pipeline_runs(pipeline=pipeline, page=page, limit=page_size)

    @router.post("/pipeline/{id}/runs")
    async def run_pipeline(self, id):
        pipeline = await self.get_pipeline(id)
        return await PipelinesService.run_pipeline(pipeline, self.project.id)
