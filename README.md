# ETL with MongoDB Atlas and AWS Glue Studio (through Pyspark scripts)

## Introduction
The amount of information is growing minute by minute and storing the volumes of data is paramount for any analytics or business intelligence. Enterprises are now generating the DataLake to consolidate all their federated data to a single location. 
	
The ETL (Extract Transform and Load) process is key functionality to having a well-structured process for the data lake. 
	
AWS provides various services for data transfer and AWS Glue is the prime service for their ETL offering. AWS Glue studio is also made available to have a graphical user interface to ease the ETL process.

In this document, we will demonstrate how to integrate MongoDB Atlas with the AWS Glue services. We will show a practical guide for loading the data from S3 through AWS Glue Crawler, Mapping, and Data Catalog services to MongoDB Atlas.
	
This can be extended to any of the source connectors of AWS GLue like CSV, XLS, Text, RDBMS, Stream data, etc.

This article is to demonstrate the capabilities of MongoDB Atlas and AWS Glue Studio Integration.


## [MongoDB Atlas](https://www.mongodb.com/atlas)

MongoDB Atlas is an all purpose database having features like Document Model, Geo-spatial , Time-seires, hybrid deployment, multi cloud services. It evolved as "Developer Data Platform", intended to reduce the developers workload on development and management the database environment. It also provide a free tier to test out the application / database features.


## [AWS Glue Studio](https://docs.aws.amazon.com/glue/latest/ug/what-is-glue-studio.html)
AWS Glue Studio is a new graphical interface that makes it easy to create, run, and monitor extract, transform, and load (ETL) jobs in AWS Glue. You can visually compose data transformation workflows and seamlessly run them on AWS Glue’s Apache Spark-based serverless ETL engine. You can inspect the schema and data results in each step of the job.

## Integration Features

With AWS Glue Studio, we can now create scripts for integrations with all the data source. In this module, we utilized the MongoDB Atlas's Spark connectors to connect to the MongoDB Atlas.
As of now there are no director connectors are available in AWS Glue Studio to connect to MongoDB Atlas. 

## Steps for Integration

### 1.Creation of MongoDB Atlas
First and foremost we need to create a MongoDB Atlas cluster (free tier is also available) for setting the MDB Atlas as either source or sink for the ETL process.

There are multiple ways for creating a MongoDB Atlas cluster in AWS.
  1. [Cloudformation Template](https://aws.amazon.com/quickstart/architecture/mongodb-atlas/)
  2. [MongoDB Atlas console](https://www.mongodb.com/docs/atlas/getting-started/)

### 2.Setup of AWS Network components
Setup the VPC, Subnet, NAT Gateway and VPC Endpoints

Login to the AWS console and search for VPC 

Click on the VPC and click on the "Create VPC" on the top right
![](https://github.com/Babusrinivasan76/atlasgluestudiointegration/blob/main/images/VPC%20Creation/01.VPC%20Creation.png)

Select the "VPC and more" and choose the parameters as shown.
![](https://github.com/Babusrinivasan76/atlasgluestudiointegration/blob/main/images/VPC%20Creation/02.VPC%20Creationpng.png)

Click "Create".

## Summary


