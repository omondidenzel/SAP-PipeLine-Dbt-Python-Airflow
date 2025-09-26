FROM astrocrpublic.azurecr.io/runtime:3.0-12

RUN mkdir -p /home/astro/.dbt

# Copy your dbt project into Airflow home (so DAGs can access it)
COPY dags/sap_pipeline /usr/local/airflow/dbt

# Copy Python requirements
COPY requirements.txt /app/requirements.txt

COPY profiles.yml /home/astro/.dbt/profiles.yml
# RUN chown -R astro:astro /home/astro/.dbt

# Install dbt + your Python deps
RUN pip install --upgrade pip && \
    pip install virtualenv && \
    virtualenv venv && \
    venv/bin/pip install dbt-core dbt-postgres && \
    venv/bin/pip install -r /app/requirements.txt

