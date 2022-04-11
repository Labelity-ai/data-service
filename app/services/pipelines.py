from typing import List, Union
from datetime import datetime
from rq.decorators import job
from fastapi import HTTPException
import aioboto3
from odmantic import query
import graphlib

from app.schema import PipelinePostData, PipelinePatchData, QueryPipelinePost
from app.models import Pipeline, ObjectId, PipelineRun, engine, Node, RunStatus, NodeType, NodeOperation
from app.core.queue import redis
from app.core.pipeline_operations import INPUT_NODE_OPERATIONS, OUTPUT_NODE_OPERATIONS
from app.models import Project
from app.config import Config

session = aioboto3.Session()


def generate_pipeline_run_logs_s3_key(project: Project, run: PipelineRun):
    return f'{Config.PIPELINES_LOGS_FOLDER}/{project.id}/{run.pipeline_id}/{run.id}'


async def execute_node(node: Node, inputs: list):
    if node.type == NodeType.INPUT:
        operation, schema_class = INPUT_NODE_OPERATIONS[node.type]
    else:
        operation, schema_class = OUTPUT_NODE_OPERATIONS[node.type]
    parameters = schema_class(**node.payload)
    return await operation(parameters, *inputs)


@job(queue='pipelines', connection=redis)
async def _run_pipeline(pipeline_id: ObjectId, nodes: List[Node]):
    outputs = []

    for i, node in enumerate(nodes):
        inputs = [outputs[x] for x in node.input_nodes]
        results = execute_node(node, inputs)
        outputs[i] = results

    run = await engine.find_one(PipelineRun, PipelineRun.pipeline_id == pipeline_id)
    run.finished_at = datetime.now()
    await engine.save(run)


def _check_graph_task_ordering(nodes: List[Node]):
    output_nodes = [i for i, n in enumerate(nodes) if n.type == NodeType.OUTPUT]
    found_non_query_node = False

    for i, node in enumerate(nodes):
        if found_non_query_node and node.operation == NodeOperation.QUERY_PIPELINE:
            raise HTTPException(400, f'QueryPipeline nodes should be executed before any other node')

        if node.operation != NodeOperation.QUERY_PIPELINE:
            found_non_query_node = True

        if any(j in output_nodes for j in node.input_nodes):
            raise HTTPException(400, f'Node {i} depends on an output node')


def _merge_consecutive_query_pipeline_nodes(nodes: List[Node]) -> List[Node]:
    query_nodes = [n for n in nodes if n.operation == NodeOperation.QUERY_PIPELINE]
    non_query_nodes = [n for n in nodes if n.operation != NodeOperation.QUERY_PIPELINE]
    steps = []

    for node in query_nodes:
        payload = QueryPipelinePost(**node.payload)
        steps.extend(payload.steps)

    merged_node = Node(
        input_nodes=[],
        type=NodeType.INPUT,
        operation=NodeOperation.QUERY_PIPELINE,
        payload=QueryPipelinePost(steps=steps).dict(),
    )

    return [merged_node] + non_query_nodes


def _check_graph_correctness(pipeline: Pipeline) -> List[Node]:
    try:
        graph = {i: set(node.input_nodes) for i, node in enumerate(pipeline.nodes)}
        sorter = graphlib.TopologicalSorter(graph)
        order = sorter.static_order()
        nodes = [pipeline.nodes[i] for i in order]
        _check_graph_task_ordering(nodes)
        return nodes
    except graphlib.CycleError as error:
        raise HTTPException(400, f'Pipeline contains cycles. Detail: {error}')


class PipelinesService:
    @staticmethod
    async def create_pipeline(pipeline: PipelinePostData, project: Project) -> Pipeline:
        pipeline_instance = Pipeline(**pipeline.dict(), project_id=project.id, prefect_flow_id='')
        _check_graph_correctness(pipeline_instance)
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
        _check_graph_correctness(pipeline)
        return await engine.save(pipeline)

    @staticmethod
    async def run_pipeline(pipeline: Pipeline) -> PipelineRun:
        nodes = _check_graph_correctness(pipeline)
        nodes = _merge_consecutive_query_pipeline_nodes(nodes)
        job = _run_pipeline.delay(pipeline.id, nodes)

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
            data = s3_client.get_object(Bucket=Config.PIPELINES_LOGS_BUCKET, Key=key)
            contents = data['Body'].read()
            return contents.decode("utf-8")

    @staticmethod
    async def add_scheduler(pipeline_id: ObjectId, project: Project):
        # TODO
        pass

    @staticmethod
    async def remove_scheduler(scheduler_id: ObjectId):
        # TODO
        pass
