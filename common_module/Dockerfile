FROM python:3.8

COPY pyproject/ .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir poetry==1.2.0 && poetry config virtualenvs.create false

RUN poetry install --with common_module
