
FROM python:latest

RUN apt update && apt install -y cron && apt install -y nano


WORKDIR /extraction

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT [ "./entrypoint.sh"]
