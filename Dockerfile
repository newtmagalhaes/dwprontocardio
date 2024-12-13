FROM quay.io/astronomer/astro-runtime:12.2.0

USER root

# Setup Oracle Instant Client
COPY setup_instant_client.sh /usr/local/airflow/setup_instant_client.sh
RUN sh /usr/local/airflow/setup_instant_client.sh

# Setup Postgres Instant Client
COPY setup_instant_client_postgres.sh /usr/local/airflow/setup_instant_client_postgres.sh
RUN sh /usr/local/airflow/setup_instant_client_postgres.sh

# Definir as variáveis de ambiente para Oracle e Postgres Instant Client
ENV LD_LIBRARY_PATH=/usr/local/airflow/.oracle/instantclient_19_23/
ENV LD_LIBRARY_PATH_POSTGRES=/usr/local/airflow/.postgresql/data/instantclient_17_0/
ENV PYTHONUNBUFFERED 1

# Instalar dependências de compilação e desenvolvimento, incluindo o GCC
RUN apt-get update && apt-get install -y \
    gcc \
    libaio1 \
    libpq-dev \
    libssl-dev \
    python3-dev \
    musl-dev \
    build-essential \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*


WORKDIR "/usr/local/airflow"
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
pip install --no-cache-dir -r requirements.txt
    

RUN chown -R astro:astro /usr/local/airflow && \
chmod -R 775 /usr/local/airflow

# WORKDIR "/usr/local/airflow/dags/dbt/dbt_nutricao"
COPY dbt_requirements.txt ./usr/local/airflow/dags/dbt
RUN pip install --no-cache-dir --upgrade pip && \
python3 -m venv /usr/local/airflow/dags/dbt/dbt_venv && \
source /usr/local/airflow/dags/dbt/dbt_venv/bin/activate && \
pip install --no-cache-dir -r dbt_requirements.txt && deactivate

# Setar o PATH para o ambiente virtual do dbt
ENV PATH="/usr/local/airflow/dags/dbt/dbt_venv/bin:$PATH"


USER astro