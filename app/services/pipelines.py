from typing import List
from datetime import datetime
from rq.decorators import job

from app.schema import PipelinePostData
from app.models import Pipeline, ObjectId, PipelineRun, engine, Node, Edge, RunStatus, NodeType
from app.core.queue import redis
from app.models import Project


def check_unconnected_nodes(nodes: List[Node], edges: List[Edge]):
    connected_nodes = set(edge.input_node for edge in edges) | set(edge.output_node for edge in edges)
    return [node for i, node in enumerate(nodes) if i not in connected_nodes]


@job(queue='pipelines', connection=redis)
async def _run_pipeline(pipeline: Pipeline):

    data_processing_nodes = [node for node in pipeline.nodes if node.type == NodeType.PROCESSING]




    input_nodes = [node for node in pipeline.nodes if node.type == NodeType.INPUT]
    output_nodes = [node for node in pipeline.nodes if node.type == NodeType.OUTPUT]

    # TODO Pipeline composition and execution
    run = await engine.find_one(PipelineRun, PipelineRun.pipeline_id == pipeline.id)
    run.finished_at = datetime.now()
    await engine.save(run)


def check_graph_cycles(nodes: List[Node], edges: List[Edge]):
    in_degrees = {}

    for node in nodes:
        in_degrees[node] = 0

    for edge in edges:
        in_degrees[nodes[edge.output_node]] += 1

    queue = []

    for node, in_degree in in_degrees.items():
        if in_degree == 0:
            queue.append(node)

    counter = 0

    while queue:
        nu = queue.pop(0)
        nu_id = [i for i, node in enumerate(nodes) if node == nu][0]
        neighbors = [nodes[edge.input_node] for edge in edges if edge.input_node == nu_id]

        for v in neighbors:
            in_degrees[v] -= 1

            if in_degrees[v] == 0:
                queue.append(v)
        counter += 1

    return counter != len(nodes)


class PipelinesService:
    @staticmethod
    async def create_pipeline(pipeline: PipelinePostData, project_id: ObjectId) -> Pipeline:
        pipeline_instance = Pipeline(**pipeline.dict(), project_id=project_id, prefect_flow_id='')
        has_cycles = check_graph_cycles(pipeline.nodes, pipeline.edges)
        unconnected_nodes = check_unconnected_nodes(pipeline.nodes, pipeline.edges)

        """
        TODO:
            - Check graph consistency (preprocessing -> transformations -> inference)
        """
        pipeline_instance = await engine.save(pipeline_instance)
        return await engine.save(pipeline_instance)

    @staticmethod
    async def get_pipelines(project_id: ObjectId, page: int, limit: int) -> List[Pipeline]:
        return await engine.find(Pipeline, Pipeline.project_id == project_id, limit=limit, skip=page * limit)

    @staticmethod
    async def get_pipeline(id: ObjectId) -> List[Pipeline]:
        return await engine.find_one(Pipeline, Pipeline.id == id)

    @staticmethod
    async def remove_pipeline(pipeline: Pipeline):
        pipeline.deleted = True
        engine.save(pipeline)

    @staticmethod
    async def run_pipeline(pipeline: Pipeline) -> PipelineRun:
        job = _run_pipeline(pipeline)
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
    async def get_pipeline_runs(pipeline: Pipeline, page: int, limit: int):
        return await engine.find(
            PipelineRun,
            PipelineRun.pipeline_id == pipeline.id,
            limit=limit,
            skip=page * limit)

    @staticmethod
    async def get_pipeline_run_logs(run: PipelineRun):
        # TODO
        pass

    @staticmethod
    async def add_scheduler(pipeline_id: ObjectId, project: Project):
        # TODO
        pass

    @staticmethod
    async def remove_scheduler(scheduler_id: ObjectId):
        # TODO
        pass
