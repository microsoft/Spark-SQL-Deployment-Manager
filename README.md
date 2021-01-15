[![Build Status](https://microsoftit.visualstudio.com/OneITVSO/_apis/build/status/Compliant/Core%20Services%20Engineering%20and%20Operations/Corporate%20Functions%20Engineering/Professional%20Services/PS%20Data%20And%20Insights/Data%20and%20Integration%20Platforms/PSDI%20Data%20Processing/PS-OMI-DAIP-DProc-MtStr-MetaStore_Build?branchName=master)](https://microsoftit.visualstudio.com/OneITVSO/_build/latest?definitionId=29958&branchName=master)
[![MIT License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/microsoft/Spark-SQL-Deployment-Manager/blob/main/LICENSE)
## Overview
  - Build and deploy a project with Spark SQL schema and tables incrementally.
  - Checks for syntax errors using Continuous Integration, preventing incorrect table schema from being merged to deployment branch.  
  - Focus on table schema changes and use this project for the deployment.
  - This project currently supports azure data lake as storage source for delta tables and Databricks implementation of Spark.

## How to use
### Create a Spark Sql Project
  - Create a spark sql project which contains details about project files like Schema and Table scripts.
  - Add pre and post deployment scripts if needed. These scripts are Scala Notebooks. The Pre deployment and Post deployment notebooks should be executed before and after executing the deployment respectively.
  - Add Azure data lake path in values.json file.
  - Refer to the exampleSqlProject Spark SQL project as a starting point.

### Building the Project 
#### Features

  - Build will check for syntax errors in the project
  - Once the build succeeds, it will create a build artifact which can be used to deploy the changes ( by invoking DeploymentManager)
#### How to build

  - BuildSql project creates BuildSql.jar file.
  - Use BuildSql.jar like an executable to build spark sql project - Run the jar by passing .sparkSql project file as command line arguments. 
  - Once the build succeeds, the build artifact can be found in bin folder created in project root directory.

### Deploying the project
#### Features
  - Based on changes in the Spark SQL project file, it will create or modify tables and schemas 
  - Currently Supports Delta table Deployment.
  - Executes Pre and Post Deployment Notebooks (used for one-time manual changes).
#### How to deploy
  - DeploymentManager project creates the DeploymentManager.jar file
  - Execute the DeploymentManager jar on the spark cluster by passing path to output.json (build artifact) as jar argument.
  - Execute Pre and Post Deployment Notebooks on the cluster before and after executing the DeploymentManager jar respectively.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
