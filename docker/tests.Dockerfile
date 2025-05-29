FROM harmony/filtering:latest

USER root
RUN pip3 install pytest

# Ensure /app exists and is owned by dockeruser
RUN mkdir -p /app && chown dockeruser:dockeruser /app

USER dockeruser
ENV PYTHONPATH=/worker

# Copy support files *as* dockeruser so they remain writable
COPY --chown=dockeruser:dockeruser tests  /app/tests
COPY --chown=dockeruser:dockeruser config /app/config
COPY --chown=dockeruser:dockeruser utils  /app/utils

WORKDIR /app
ENTRYPOINT ["pytest","--maxfail=1","--disable-warnings","-q"]
