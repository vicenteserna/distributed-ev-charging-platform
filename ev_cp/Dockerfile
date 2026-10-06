FROM python:3.9-slim

WORKDIR /app

COPY ev_cp/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ev_cp/ .
COPY ev_common/ ./ev_common/

ENV PYTHONPATH=/app

CMD ["python", "app.py"]
