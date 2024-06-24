# Medallion architecture Lake with ETL 

#### A simple project which objective extract data from an API and perform an ETL for a data lake built based on a medallion architecture.

# Directories and files

## Repository folder structure

```bash
├── test
│   └── unit_test.py
├── utils
│   └── python_functions.py
├── bronze_to_silver.py
├── silver_to_gold.py
└── check_results.py
```

## Files

    unit_test.py: file responsible for unit test coverage of functions created for ETL.

    python_functions.py: contains all the main functions for running ETL.

    bronze_to_silver.py: file responsible for transitioning data from bronze to silver layer. Converts information stored in json to partitioned columnar format. It also performs cleaning of completely null columns.

    silver_to_gold.py: file responsible for transitioning data from silver to gold layer. Creates an aggregated view with the quantity of breweries per type and location and save in a partitioned columnar format. By location I considered "postal_code" column. But could be a composition of more partitions like "country", "state", etc.
    
    check_results.py: small script to check the results in silver and gold layers.

# Microsoft Azure resources

    Subscription: Pay-As-You-Go subscription. Type of subscription where you only pay for the resources used.

    Storage account: scalable and durable storage for data in Azure.

    Azure Data Factory: cost-efficient and fully managed serverless cloud data integration tool that scales on demand.

    Azure Databricks: collaborative Apache Spark-based analytics platform with scalable resources.

# Setting up the environment

    1. Storage account: to create an storage account go to "create a resource" in top left corner of the console.
        1.1 Search for "storage account".
        1.2 Choose and click in "create".
        1.3 The storage account configuration used in this project was "standard" and "locally-redundant storage".

    2. Azure Data Factory: to create an storage account go to "create a resource" in top left corner of the console.
        2.1 Search for "data factory".
        2.2 Choose and click in "create".
        2.3 The data factory configuration for this project was the default options for all possibilities including Networking configuration via "public endpoint".

    3. Azure Databricks: to create an storage account go to "create a resource" in top left corner of the console.
        3.1 Search for "azure databricks".
        3.2 Choose and click in "create".
        3.3 Keep default configurations except "Pricing tier" which should be change to "standard"

    4. Medallion architecture
        4.1 Inside storage account create 3 containers (bronze, silver and gold).
        4.2 All container where created with access level set to "private".

    5. Pipeline
        5.1 In data factory studio go to pipeline and crete one (drag and drop).
        5.2 The first resource will be "Copy data".
        5.3 Here create a a new "source dataset" and serach for "Rest". Then pass the url of the api. In "sink" option create another dataset with "Azure Data Lake Storage Gen2" and chosse "json" format. Configure the preferible path to sink data in the bronze container.
        5.4 The other two steps of this pipeline are Databricks Notebooks. Just create the two resources from "Databricks" > "Notebbok" and link both notebooks in each job.
        5.5 Link each job with the option of "on success"
        5.6 the final pipeline should be logically linked as: "Call Api" > "on success" > "Bronze to Silver" > "on sucess" > "Silver to Gold".
        5.7 Create a trigger in the option "trigger" > "New/Edit".


# Monitoring and alerting

    As mentioned in Azure documentation: https://learn.microsoft.com/en-us/azure/data-factory/monitor-data-factory#monitoring-methods. Its possible to monitor all Data Factory pipeline runs natively in Azure Data Factory Studio. To open the monitoring experience, select "Launch Studio" from your Data Factory page in the Azure portal, and in Azure Data Factory Studio, select "Monitor" from the left menu. This project has the visually monitor option enabled.

    Data Facotry has several options for alerting such as: metric alerts, log search alerts, activity log alerts, etc. In this project, log alerts were not implemented due to the associated additional cost and low need for alerts.

    Databrick allows several options for monitoring and handle data issues as mentioned in [Databricks documentation](https://www.databricks.com/discover/pages/data-quality-management). However, other tools and frameworks can be use to handle data quality. Datadog, for example, is a plataform for monitoring and security that can be integrated with Databricks. Another option is using asserts in Data flow, which is a resource inside Data Facotry. And for a more code-oriented option the framework Great Expectations allows build constraints and log the activity of the pipeline.

# Possible improvements

    1. The pipeline created for this project has 3 steps: "Call API", "Bronze to Silver" and "Silver to Gold". But could be done in two steps by linking the notebooks "Bronze to Silver" and "Silver to Gold" into a "main" notebook. Inside "main" the steps could be manage in a sequence. However, this approach makes difcult to handle parallelism for more complex pipelines (complex DAGs should be harder to debugg and execute using this last approach).

    2. Could be added an alert in Data Factory for failures executions.

    3. Mount container should be done using authentication process using credentials.

    4. The data for each execution of the pipeline is stored inside a folder /anomesdia=<current_date_of_execution>/ which is not functional partition of the data bu only a folder to separate each execution. This could be added to "Bronze to Silver" job to add a columns "anomesdia" and save the data in a parquet file also partiioned by the date of execution. This makes easier to track problems in data becouse the date of the execution is linked to the data path in ETL.

    5. To go further with this project one could connect a Power Bi to gold layer to build a dashboard, or an API to give access to this data outside of the account.

    6. Adding a dataquality step would be a huge improvement. Specially if the dataquality step has an conditional option which, promote data downstream of the pipeline if pass the quality constraints or sink in another container for "augmentation". Every data quality check should be log in a dataquality tables inside the layer which data is landing.