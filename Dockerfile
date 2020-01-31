FROM python:3.7

# Autor
LABEL maintainer='martin.mulone@moneyonchain.com'

RUN apt-get update && \
    apt-get install -y \
        locales \
        supervisor 

RUN echo $TZ > /etc/timezone && \
    apt-get update && apt-get install -y tzdata && \
    rm /etc/localtime && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir /home/www-data && mkdir /home/www-data/app \
    && mkdir /home/www-data/app/moc_jobs \
    && mkdir /home/www-data/app/moc_jobs/logs

# We start copying all the files inside the container as AWS FARGATE does not support volumes

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

WORKDIR /home/www-data/app/moc_jobs/
#COPY build ./build
COPY moc_jobs.py ./
COPY contracts_manager.py ./
COPY config.json ./
ENV PATH "$PATH:/home/www-data/app/moc_jobs/"
ENV PYTHONPATH "${PYTONPATH}:/home/www-data/app/moc_jobs/"
CMD ["/usr/bin/supervisord"]
