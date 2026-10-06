FROM python:3.9-slim

WORKDIR /app

COPY ev_central/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ev_central/ .
COPY ev_common/ ./ev_common/

ENV PYTHONPATH=/app

CMD ["python", "app.py"]
