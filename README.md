# CDE Data Gen

## Objective

This project provides templates for generating realistic, synthetic data at scale using DBLDatagen and Spark in Cloudera Data Engineering. The templates are customizable according to your use case. The templates are automatically deployed via scripts that wrap around the CDE CLI.

## Table of Contents

* [Requirements]()
* [Important Information]()
* [Deployment Instructions]()
* [Teardown Instructions]()
* [Summary]()

## Requirements

To deploy the demo via this automation you need:

* A CDP tenant in Public or Private cloud.
* A CDP Workload User with Ranger policies and IDBroker Mappings configured accordingly.
* An CDE Service on version 1.21 or above.
* The Docker Custom Runtime entitlement. Please contact the CDE product or sales team to obtain the entitlement.
* A Dockerhub account. Please have your Dockerhub user and password ready.

## Important Information

The automation deploys the following to your CDE Virtual Cluster:

* A CDE Spark Job and associated CDE Resources with the purpose of creating synthetic data in Cloud Storage for each participant.
* A CDE Files Resource for Spark files shared by all participants named "Spark-Files-Shared".
* A CDE Files Resource for Airflow files shared by all participants named "Airflow-Files-Shared".
* A CDE Python Resource shared by all participants named "Python-Env-Shared".

## Deployment Instructions

When setup is complete navigate to the CDE UI and validate that the job run has completed successfully. This implies that the HOL data has been created successfully in Cloud Storage.

Clone this repository to your machine. Then run the deployment script with:

```
% ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

For example:

```
#AWS
% ./setup/deploy_hol.sh pauldefusco pauldefusco s3a://goes-se-sandbox01/data
```

```
#Azure
% ./setup/deploy_hol.sh pauldefusco pauldefusco abfs://logs@go01demoazure.dfs.core.windows.net/data
```

## Teardown Instructions

When you are done run this script to tear down the data in the Catalog but not in S3. That step will be handles by the GOES teardown scripts.

```
% ./teardown.sh cdpworkloaduser
```

## Summary

You can deploy an end to end CDE Demo with the provided automation. The demo executes a small ETL pipeline including Iceberg, Spark and Airflow.
