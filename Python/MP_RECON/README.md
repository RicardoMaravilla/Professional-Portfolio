# ODS Database Recon

Python app to do a reconciliation based on the records in the database recon table.

## Image

txo-dswim-esb-docker-local.artifactory.swg-devops.com/database-recon

## Files and Description

- constants.py
- database.py
- utils.py
- secrets_manager.py : Class with methods to get the secrets from Secrets Manager IBM Cloud.

## Usage

### Apps

- Refeed Pricing Marketplace
- Streaming Pricing Marketplace

### Tables

- FFXT1.MARKETPLACE_RECON


### Instructions to build loadreport cronJob

1 Move to ~/ODS/Python/MP_RECON folder

2 Copy requirements.txt file and keystore folder from [this box folder](https://ibm.ent.box.com/folder/229395831836) to your current path in your terminal.

3 From your current path, build load-report-mp image by running the following commands

    docker build -t docker-na-private.artifactory.swg-devops.com/txo-dswim-esb-docker-local/load-report-mp:<Image Version> -f Dockerfile .
    docker push docker-na-private.artifactory.swg-devops.com/txo-dswim-esb-docker-local/load-report-mp:<Image Version>
    
4 Update cronjob.yaml file to reference the new image version, as well as the respective namespace (loadreport-qa for Testing, loadreport for PROD) and upload changes to the required kubernetes cluster.