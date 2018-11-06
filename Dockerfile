FROM python:3.7.1-stretch

ADD ./ /code
WORKDIR /code

ENTRYPOINT ["./docker-entrypoint.sh"]
