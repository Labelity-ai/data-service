FROM python:3.7

WORKDIR /app

COPY requirements.txt .
COPY app ./app
COPY server.py .

RUN pip install -r requirements.txt

CMD [ "PYTHON_PATH=. python", "./server.py" ]
