FROM python:3.7.1-stretch

RUN echo 'alias pytest="python setup.py pytest"' >> ~/.bashrc

ADD ./ /code
WORKDIR /code
RUN python setup.py develop

ENTRYPOINT ["./docker-entrypoint.sh"]
