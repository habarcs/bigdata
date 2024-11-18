FROM python:3.13.0-slim-bookworm

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./data_gen

CMD [ "python", "-m", "data_gen" ]
