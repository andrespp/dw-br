FROM python:3.8
MAINTAINER Andre Pereira andrespp@gmail.com

RUN apt-get update && apt-get install -y vim && cd ~/ && \
 wget https://raw.githubusercontent.com/andrespp/dotfiles/master/.vimrc-basic && \
 mv .vimrc-basic .vimrc

RUN apt-get -y install apt-transport-https unixodbc-dev curl && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > \
		/etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get -y install msodbcsql17 mssql-tools && \
	apt-get clean && rm -rf /var/lib/apt/list && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc && \
    /bin/bash -c "source ~/.bashrc"

COPY ./requirements.txt ./

RUN pip install -r requirements.txt

COPY . .

WORKDIR /usr/src/app

ENTRYPOINT ["./entrypoint.sh"]
CMD ["help"]
