# Labelity Data Service

## Architecture

![](docs/architecture.png)

## Features

- REST API for storing and retrieving annotations, predictions and metadata
- S3 integration for image and video storage
- Annotations and images exporting in common formats (COCO, CVAT XML, VOC, etc)
- Extensible pipeline engine for selecting, filtering and transforming data:
  - Data processing nodes: Transformations over annotations (executed by DB engine)
  - Data Augmentation nodes: Transformations over images + annotations  (executed by serverless worker node)
  - Custom nodes: Any Prefect task

## How to install for development?

### 1. Install server dependencies

```bash
pip3 -m venv env
source ./env/bin/activate
pip3 install -r requirements.txt
```

### 2. Setup serverless functions

TO-DO


### 3.s
