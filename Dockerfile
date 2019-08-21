FROM python:3-alpine
MAINTAINER Tarjei N Skrede "tarjei.skrede@sesam.io"

COPY ./service /service
WORKDIR /service

RUN pip install --upgrade pip && pip install -r requirements.txt && chmod -x ./service.py

EXPOSE 5000/tcp

ENTRYPOINT ["python"]
CMD ["service.py"]
