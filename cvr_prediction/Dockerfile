# syntax=docker/dockerfile:1.2
FROM python:3.8-buster as builder

ARG NO_DEV

COPY pyproject/ .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir poetry==1.2.0 && poetry config virtualenvs.create false
RUN --mount=type=cache,id=custom-poetry,mode=0755,target=/root/.cache/poetry \
    if [ -n "$NO_DEV" ]; then \
        poetry install --with sophia-ai --without dev; \
    else \
        poetry install --with sophia-ai; \
    fi

FROM python:3.8-slim-buster as runner
# for timezone
RUN apt-get install -y tzdata
COPY --from=builder /usr/local/lib/python3.8/site-packages /usr/local/lib/python3.8/site-packages
# for LightGBM
COPY --from=builder /usr/lib/x86_64-linux-gnu/libgomp.so.1 /usr/lib/x86_64-linux-gnu/libgomp.so.1
WORKDIR /usr/src/app

COPY cvr_prediction/ .
