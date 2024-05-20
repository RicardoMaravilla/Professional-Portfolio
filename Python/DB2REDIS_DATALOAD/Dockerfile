FROM python:3.8
USER root
    # set the working directory in the container to /app
    WORKDIR /app

    ADD ./requirements.txt /app/requirements.txt
    RUN pip3.8 install --trusted-host pypi.python.org -r /app/requirements.txt

    # add the current directory to the container as /app
    ADD . /app

    # set timezone to ET
    ENV TZ 'America/New_York'

    # execute the Flask app
    CMD ["python", "src/redis_dataload.py", "-f"]
