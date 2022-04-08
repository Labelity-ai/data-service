from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends
from odmantic import ObjectId

from app.schema import PipelineRunInfo, PipelinePostData, PipelinePatchData
from app.models import Project
from app.security import get_project
from app.services.pipelines import PipelinesService

router = InferringRouter(
    tags=["pipeines"],
)


@cbv(router)
class PipelinesView:
    project: Project = Depends(get_project)

    @router.get("/pipelines/")
    async def get_pipelines(self):
        return await PipelinesService.get_pipelines(project=self.project)

    @router.get("/pipelines/{id}")
    async def get_pipeline(self, id):
        return await PipelinesService.get_pipeline(id=id)

    @router.post("/pipelines/")
    async def create_pipeline(self, data: PipelinePostData):
        return await PipelinesService.create_pipeline(pipeline=data, project=self.project)

    @router.put("/pipelines/{id}")
    async def replace_pipeline(self, id: ObjectId, data: PipelinePostData):
        pipeline = await self.get_pipeline(id)
        return await PipelinesService.update_pipeline(pipeline=pipeline, data=data)

    @router.patch("/pipelines/{id}")
    async def update_pipeline(self, id: ObjectId, data: PipelinePatchData):
        pipeline = await self.get_pipeline(id)
        return await PipelinesService.update_pipeline(pipeline=pipeline, data=data)

    @router.delete("/pipelines/{id}")
    async def delete_pipeline(self, id):
        pipeline = await self.get_pipeline(id)
        return await PipelinesService.delete_pipeline(pipeline)

    @router.get("/pipelines/{id}/runs")
    async def get_pipeline_runs(self, id):
        pipeline = await self.get_pipeline(id)
        return await PipelinesService.get_pipeline_runs(pipeline=pipeline)

    @router.post("/pipeline/{id}/runs")
    async def run_pipeline(self, id) -> PipelineRunInfo:
        pipeline = await self.get_pipeline(id)
        run = await PipelinesService.run_pipeline(pipeline)
        return PipelineRunInfo(*run.dict(exclude=['job_id']))

    @router.get("/pipeline/runs/{id}/logs")
    async def get_pipeline_run_logs(self, id):
        run = await PipelinesService.get_pipeline_run(id)
        return await PipelinesService.get_pipeline_run_logs(run, self.project)
