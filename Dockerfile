FROM node:8.9.0
MAINTAINER Bratan

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY package.json /usr/src/app
RUN npm install

COPY . /usr/src/app

CMD [ "npm", "start" ]