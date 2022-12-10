FROM python:3.8-bullseye

# install awscli
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip && ./aws/install

# install aws cdk
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash -
RUN apt-get install -y nodejs
RUN npm install -g aws-cdk

COPY . .
RUN pip3 install -r requirements.txt
RUN pip3 install -r requirements-dev.txt
