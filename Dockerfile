FROM continuumio/miniconda3
MAINTAINER Andre Pereira andrespp@gmail.com

RUN apt-get update && apt-get install -y vim build-essential && cd ~/ && \
 wget https://raw.githubusercontent.com/andrespp/dotfiles/master/.vimrc-basic && \
 mv .vimrc-basic .vimrc

RUN apt-get -y install unixodbc-dev python3-psycopg2 libpq-dev

# Setup Conda Environment
ARG CONDA_ENV_NAME=dwbra
COPY ./environment.yml ./
RUN conda env create -f environment.yml
RUN echo "source activate $CONDA_ENV_NAME" > ~/.bashrc
ENV PATH /opt/conda/envs/$CONDA_ENV_NAME/bin:$PATH

COPY . .

WORKDIR /usr/src/app

ENTRYPOINT ["./entrypoint.sh"]
CMD ["help"]
