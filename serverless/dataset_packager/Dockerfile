FROM python:3.8-buster

ARG FUNCTION_DIR="/function"

RUN mkdir -p ${FUNCTION_DIR}

WORKDIR ${FUNCTION_DIR}

RUN apt-get update && \
  apt-get install -y \
  gcc \
  g++ \
  git \
  make \
  cmake \
  unzip \
  libcurl4-openssl-dev

COPY handler.py ${FUNCTION_DIR}/handler.py
COPY requirements.txt ${FUNCTION_DIR}/requirements.txt

RUN pip install --target ${FUNCTION_DIR} awslambdaric
RUN pip3 install -r requirements.txt

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD ["handler.main"]
