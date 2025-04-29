# Add copyright statement here

FROM ghcr.io/osgeo/gdal:alpine-small-3.9.3

# System dependencies and Python setup
RUN apk add bash build-base gcc g++ gfortran openblas-dev cmake \
        python3 python3-dev libffi-dev netcdf-dev libxml2-dev \
        libxslt-dev libjpeg-turbo-dev zlib-dev hdf5 hdf5-dev

RUN python3 -m venv /service-env
ENV PATH="/service-env/bin:${PATH}"

RUN python3 -m ensurepip --upgrade
RUN pip3 install numpy netCDF4 matplotlib harmony-service-lib
RUN pip3 install "gdal==3.9.3"

# Create a non-root user
RUN adduser -D -s /bin/sh -h /home/dockeruser -g "" -u 1000 dockeruser
ENV HOME /home/dockeruser
USER dockeruser

# Prepare workdir
USER root
RUN mkdir -p /worker && chown dockeruser /worker
USER dockeruser

WORKDIR /worker

# Copy your package from the src/ layout
COPY --chown=dockeruser src/harmony_filtering_service/ /worker/harmony_filtering_service/

# Run the adapter module directly
ENTRYPOINT ["python3", "-m", "harmony_filtering_service.adapter"]


