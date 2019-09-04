FROM python:3-alpine
MAINTAINER Tarjei N Skrede "tarjei.skrede@sesam.io"

COPY ./service /service
WORKDIR /service

RUN apk add build-base &&  pip install --upgrade pip && pip install -r requirements.txt && chmod -x ./service.py && \
apk del build-base && apk add 'libstdc++'

EXPOSE 5000/tcp

ENTRYPOINT ["python"]
CMD ["service.py"]
