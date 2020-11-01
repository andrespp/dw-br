FROM continuumio/miniconda3
MAINTAINER Andre Pereira andrespp@gmail.com

RUN apt-get update && apt-get install -y vim build-essential && cd ~/ && \
 wget https://raw.githubusercontent.com/andrespp/dotfiles/master/.vimrc-basic && \
 mv .vimrc-basic .vimrc

#RUN apt-get -y install apt-transport-https unixodbc-dev curl
RUN apt-get -y install unixodbc-dev python3-psycopg2 libpq-dev

COPY ./environment.yml ./

RUN conda env update -f environment.yml

COPY . .

WORKDIR /usr/src/app

ENTRYPOINT ["./entrypoint.sh"]
CMD ["help"]
