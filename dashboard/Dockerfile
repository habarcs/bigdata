FROM python:3.9-slim

WORKDIR /app

COPY . ./dashboard 

RUN pip install --no-cache-dir -r dashboard/requirements.txt

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["streamlit", "run", "dashboard/home.py", "--server.port=8501", "--server.address=0.0.0.0"]
