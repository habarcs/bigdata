FROM python:3.13.0-slim-bookworm

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

CMD [ "python", "-u", "-m", "flask", "run", "--host=0.0.0.0"]
