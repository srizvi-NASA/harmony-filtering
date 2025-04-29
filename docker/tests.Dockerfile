# Use your freshly built service image
FROM harmony/filtering:latest

# install pytest...
USER root
RUN pip3 install pytest
USER dockeruser

COPY tests /app/tests
WORKDIR /app

ENTRYPOINT ["pytest","--maxfail=1","--disable-warnings","-q"]

