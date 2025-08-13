FROM python:3.9.7
LABEL maintainer="Eduardo Castillero"

ARG NAME=autores_cms
ENV NAME ${NAME}

# install core libraries
RUN apt-get update

# install application libraries
RUN pip install --upgrade pip && pip install \
    numpy==1.24.1 \
    pandas==1.4.1 \
    pyarrow==15.0.2 \
    google-api-python-client==2.42.0 \
    google-auth==2.23.2 \
    google-auth-httplib2==0.1.0 \
    google-cloud-bigquery==3.20.1 \
    google-cloud-language==2.17.0 \
    db-dtypes==1.4.1 \
    tqdm==4.64.1

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents .
VOLUME ./data

# Set user for container security
RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

# Launch script
CMD ["python", "main.py"]