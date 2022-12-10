FROM amazon/aws-lambda-python:3.8

ARG NO_DEV

# for LightGBM
RUN yum install -y libgomp
# for timezone
RUN yum install -y tzdata

WORKDIR ${LAMBDA_TASK_ROOT}

COPY pyproject/ .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir poetry==1.2.0 && poetry config virtualenvs.create false
RUN if [ -n "$NO_DEV" ]; then \
        poetry install --with sophia-ai,main --without dev; \
    else \
        poetry install --with sophia-ai,main; \
    fi

COPY main/ ${LAMBDA_TASK_ROOT}

CMD [ "lambda_handler.lambda_handler" ]
