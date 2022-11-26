FROM apache/airflow:2.3.2-python3.9
LABEL maintainer="Pedro Simas Neto"
USER root

# Atualiza SO
RUN apt-get update \
  && apt-get install -y --no-install-recommends \ 
    python3.9-dev \
    python3-pip \
  && ACCEPT_EULA=Y apt-get -y upgrade \
  && apt-get autoremove -yqq --purge \
  && apt-get clean

# Instalar pacotes necessários
RUN apt-get install -y --no-install-recommends \
    wget \
    unzip \
    libaio1

# Configurar pacotes do Oracle Client
RUN sudo mkdir -p /opt/oracle/ \
  && cd /opt/oracle && sudo wget https://download.oracle.com/otn_software/linux/instantclient/214000/instantclient-basic-linux.x64-21.4.0.0.0dbru.zip \
  && sudo unzip ** && rm -f *.zip \
  && sudo sh -c "echo /opt/oracle/instantclient_21_4 > /etc/ld.so.conf.d/oracle-instantclient.conf" \
  && sudo ldconfig
  # && export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_4:$LD_LIBRARY_PATH

USER airflow

# Atualiza pip
RUN pip install --no-cache-dir --upgrade pip

# Instalação de novos pacotes
RUN pip install --no-cache-dir \
  apache-airflow[telegram]==2.3.2 \
  apache-airflow[oracle]==2.3.2 \
  apache-airflow[microsoft.mssql]==2.3.2 \
  cx_Oracle==8.3.0 \
  firebirdsql==1.2.2 \
  openpyxl==3.0.10
