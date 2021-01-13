[![Build Status](https://microsoftit.visualstudio.com/OneITVSO/_apis/build/status/Compliant/Core%20Services%20Engineering%20and%20Operations/Corporate%20Functions%20Engineering/Professional%20Services/PS%20Data%20And%20Insights/Data%20and%20Integration%20Platforms/PSDI%20Data%20Processing/PS-OMI-DAIP-DProc-MtStr-MetaStore_Build?branchName=master)](https://microsoftit.visualstudio.com/OneITVSO/_build/latest?definitionId=29958&branchName=master)
# Build and Deploy Spark SQL Tables.
  - Build and Deploy Spark SQL tables incrementally. Checks for syntax errors before checking in the code to main and Deploy the changes using Continuous Deployment. 
  - This project aims to create a way to deploy spark sql tables using CI/CD and focus just on table schema changes rather than how to deploy the changes.
  - This project currently supports azure data lake as storage source for delta tables.

# Spark Sql Project
  - Create a spark sql project which contains details about project files like Schema and Table scripts.
  - Add pre and post deployment scripts if needed. These scripts are Scala Notebooks. The Pre deployment and Post deployment notebooks should be executed before and after executing the deployment respectively.
  - Add Azure data lake path in values.json file.
  - Refer to the exampleSqlProject spark sql project as a starting point.
  
# Build
## Features

  - SqlBuild jar helps building the spark sql project.
  - Build typically checks for syntax errors.
  - Once the build succeeds, it will create a build artifact which can be used to Deploy the changes ( by invoking Deployment Manager) 
## How to build

  - BuildSql project creates BuildSql.jar file.
  - Use BuildSql.jar like an executable to build spark sql project.
  - Run the jar by passing .sparkSql project file as command line arguments.
  - Build Artifact is generated once build succeeds. You can find this artifact in bin folder created in project root directory.

# Deploy
## Features

  - Currently Supports Delta table Deployment and  schema changes.
  - Execute Pre and Post Deployment Notebooks (typically to change anything manual or create some source data).
  
## How to deploy
  - Modify the config file in the DeploymentManager project, to pass the relevant Azure Data Lake details and Databricks Scope
  - Build the DeploymentManager jar.
  - Execute the DeploymentManager jar on the spark cluster by passing output.json (build artifact) path as jar argument.
  - Make sure to execute Pre and Post Deployment Notebooks on the cluster before and after executing the DeploymentManager jar respectively.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
