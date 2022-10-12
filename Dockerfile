FROM python:3.7

WORKDIR /app

EXPOSE 5000

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY app ./app
COPY server.py .

ENV PYTHONPATH .
ENV PORT 5000

CMD ["python", "server.py"]
