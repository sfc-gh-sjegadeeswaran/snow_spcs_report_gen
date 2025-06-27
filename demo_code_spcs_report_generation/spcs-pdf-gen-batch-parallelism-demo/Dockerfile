ARG BASE_IMAGE=continuumio/miniconda3:4.12.0   
FROM $BASE_IMAGE
RUN conda install python=3.8 && \
    conda install snowflake-snowpark-python && \
    pip install snowflake-connector-python[pandas] && \
    pip install pandas && \
    pip install fpdf2 && \
    pip install argparse
COPY main.py ./
COPY pdf_gen/ ./pdf_gen/  
ENTRYPOINT ["python3", "main.py"]