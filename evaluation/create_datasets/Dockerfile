FROM python:3.7-slim-buster

RUN python3 -m pip install --upgrade pip && pip3 install pm4py==2.2.29
RUN mkdir /app

COPY multiple_datasets_realistic.py /app/script.py
WORKDIR /app
RUN mkdir /app/input
RUN mkdir /app/output
RUN mkdir /tmp/spark-events
VOLUME /app/input
VOLUME /app/output

ENTRYPOINT ["python3","script.py"]
CMD ["/input/log_100_113.xes","10","3"]
