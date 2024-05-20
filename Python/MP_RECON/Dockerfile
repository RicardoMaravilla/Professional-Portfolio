# this is an official Python runtime, used as the parent image
FROM python:latest

# set the working directory in the container to /app
WORKDIR /app

ADD ./requirements.txt /app/requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r /app/requirements.txt && \
    mkdir ./logs && \
    useradd -u 5000 dswdiauser  && \
    chown -R dswdiauser: ./logs

# add the current directory to the container as /app
ADD . /app

# set timezone to ET
ENV TZ 'America/New_York'

# execute the Flask app
CMD ["python", "src/load_report.py", "-f"]
USER dswdiauser