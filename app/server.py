from odmantic import AIOEngine, ObjectId
from fastapi import Depends, FastAPI, HTTPException

from app.schema import ImageAnnotationsPostSchema
from app.models import ImageAnnotations
from app.security import get_project_id


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


@app.patch("/annotations/{id}", response_model=ImageAnnotations)
async def update_annotations(id: ObjectId,
                             annotation: ImageAnnotationsPostSchema,
                             project_id: ObjectId = Depends(get_project_id)):
    instance = ImageAnnotations(id=id, project_id=project_id, **annotation.dict())
    return await engine.save(instance)


@app.delete("/annotations/{id}", response_model=ImageAnnotations)
async def update_annotations(id: ObjectId, project_id: ObjectId = Depends(get_project_id)):
    annotations = await engine.find_one(
        ImageAnnotations,
        (ImageAnnotations.id == id) & (ImageAnnotations.project_id == project_id))

    if annotations is None:
        raise HTTPException(404)

    await engine.delete(annotations)
