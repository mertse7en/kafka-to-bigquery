FROM python:3.9

RUN apt update
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin
RUN apt-get install -y librdkafka-dev
RUN apt install python3-confluent-kafka
RUN pip3 install confluent-kafka

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
COPY . /app/.

ENTRYPOINT [ "python", "-u", "run.py"]