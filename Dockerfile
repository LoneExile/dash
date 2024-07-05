FROM python:alpine3.20

WORKDIR /dash

RUN apk update && apk add --no-cache \
  postgresql16-client=16.3-r0

COPY . .

RUN pip install --no-cache-dir -r requirements.txt
CMD ["tail", "-f", "/dev/null"]
