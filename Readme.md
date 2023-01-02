# Azure Blob Backup
[![Build](https://github.com/floriankramer/azure-blob-backup/actions/workflows/build.yml/badge.svg)](https://github.com/floriankramer/azure-blob-backup/actions/workflows/build.yml)

This is a rust implementation of an incremental backup service using azure blob
storage as its storage backend. 

## Docker
The repo contains a dockerfile and docker-compose file to run the service. For configuration
copy the `config.template.yaml` to `config.yaml` and insert a valid sas url. Then copy the
`docker-compose.override.template.yaml` to `docker-compose.override.yaml` and adjust the
source path of the data volume mount. This will be the directory that is backed up.
Then run `docker compose build` and `docker compose up -d` to build the docker image
and start the service.

By default backups are run every day at 1 am. Change the provided crontab to edit the scheduling.

