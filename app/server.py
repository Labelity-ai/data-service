from typing import List

from odmantic import AIOEngine, ObjectId
from fastapi import Depends, FastAPI, HTTPException, status

from app.schema import ImageAnnotationsPostSchema
from app.models import ImageAnnotations
from app.security import get_project_id
from app.config import Config

app = FastAPI()
engine = AIOEngine()


@app.get("/annotations/{id}", response_model=ImageAnnotations)
async def get_annotations_by_id(id: ObjectId, project_id: ObjectId = Depends(get_project_id)):
    annotations = await engine.find_one(
        ImageAnnotations,
        (ImageAnnotations.id == id) & (ImageAnnotations.project_id == project_id))
    if annotations is None:
        raise HTTPException(404)
    return annotations


@app.get("/annotations", response_model=ImageAnnotations)
async def get_annotations(event_id: str, project_id: ObjectId = Depends(get_project_id)):
    return await engine.find_one(
        ImageAnnotations,
        (ImageAnnotations.event_id == event_id) & (ImageAnnotations.project_id == project_id))


@app.post("/annotations", response_model=ImageAnnotations)
async def get_annotations_by_event_id(annotation: ImageAnnotationsPostSchema,
                                      project_id: ObjectId = Depends(get_project_id)):
    instance = ImageAnnotations(**annotation.dict(), project_id=project_id)
    return await engine.save(instance)


@app.post("/annotations_bulk", response_model=List[ImageAnnotations])
async def get_annotations_by_event_id(annotations: List[ImageAnnotationsPostSchema],
                                      project_id: ObjectId = Depends(get_project_id)):
    instances = [ImageAnnotations(**annotation.dict(), project_id=project_id)
                 for annotation in annotations]

    if len(annotations) > Config.POST_BULK_LIMIT:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                            f'Payload too large. The maximum number of annotations to be'
                            f' added in a single request is {Config.POST_BULK_LIMIT}')

    return await engine.save_all(instances)


@app.patch("/annotations/{id}", response_model=ImageAnnotations)
async def update_annotations(id: ObjectId,
                             annotation: ImageAnnotationsPostSchema,
                             project_id: ObjectId = Depends(get_project_id)):
    instance = ImageAnnotations(id=id, project_id=project_id, **annotation.dict())
    return await engine.save(instance)


@app.delete("/annotations/{id}", response_model=ImageAnnotations)
async def delete_annotations(id: ObjectId, project_id: ObjectId = Depends(get_project_id)):
    annotations = await engine.find_one(
        ImageAnnotations,
        (ImageAnnotations.id == id) & (ImageAnnotations.project_id == project_id))

    if annotations is None:
        raise HTTPException(404)

    await engine.delete(annotations)


@app.delete("/annotations/{id}", response_model=ImageAnnotations)
async def delete_annotations(id: ObjectId, project_id: ObjectId = Depends(get_project_id)):
    annotations = await engine.find_one(
        ImageAnnotations,
        (ImageAnnotations.id == id) & (ImageAnnotations.project_id == project_id))

    if annotations is None:
        raise HTTPException(404)

    await engine.delete(annotations)
