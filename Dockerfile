FROM continuumio/miniconda3:22.11.1

USER root

# vim
RUN apt-get update && apt-get install -y vim && cd ~/ && \
 wget https://raw.githubusercontent.com/andrespp/dotfiles/master/.vimrc-basic && \
 mv .vimrc-basic .vimrc

# Locale
RUN apt-get install -y locales && locale-gen pt_BR.UTF-8

# Setup Conda Environment
ARG CONDA_ENV_NAME=dwbr
COPY ./environment.yml ./
# RUN conda install conda=23.3.1 && conda env create -f environment.yml
# RUN conda update -n base -c defaults conda && \
RUN conda env create -f environment.yml
RUN echo "source activate $CONDA_ENV_NAME" > ~/.bashrc
ENV PATH /opt/conda/envs/$CONDA_ENV_NAME/bin:$PATH
ENV HOME /tmp

WORKDIR /usr/src/app

USER 1000

COPY . .

ENV TZ=America/Belem

ENTRYPOINT ["./entrypoint.sh"]
CMD ["help"]
