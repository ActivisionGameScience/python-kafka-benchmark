# Python Kafka Benchmarks

This repo contains the code in the form of a notebook to benchmark kafka clients.

## Install

we will use [conda](http://conda.pydata.org/) and pip install your env.

    conda create -n kafka-benchmark python=3 ipython jupyter -y
    source activate kafka-benchmark
    conda install -c activisiongamescience confluent-kafka pykafka -y # will also get librdkafka
    pip install kafka-python # pure python version is easy to install

## Kafka

You will also need a running kafka cluster. Provided is a docker-compose file to run one locally but you can spin up anyway you want.

[docker](https://www.docker.com/)
[docker-compose](https://docs.docker.com/compose/)
