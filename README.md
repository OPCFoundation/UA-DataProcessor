# UA-DataProcessor
Data Processor for the UA Cloud Initiative. Runs in a Docker container and processes data from connected data sources (see DataServices directory) and outputs the processed data as nodeset files into UA Cloud Library.

Currently, available data services are for Azure Data Explorer and Dynamics365, but other data services can be added by implementing the simple IDataService interface.

In terms of data processors, currently a Product Carbon Footprint (PCF) processor following the Green-House Gas (GHG) Protocol calculation method is implemened.

# Required Environment Variables:
- `UA_CLOUD_LIBRARY_URL`: The URL for the UA Cloud Library instance to upload nodeset files to.
- `UA_CLOUD_LIBRARY_USERNAME`: The username for authenticating with the UA Cloud Library.
- `UA_CLOUD_LIBRARY_PASSWORD`: The password for authenticating with the UA Cloud Library.

- `ADX_HOST`: The hostname for the Azure Data Explorer instance.
- `ADX_DB`: The database name for the Azure Data Explorer instance.
- `ADX_APPLICATION_ID`: The application/client ID for authenticating with Azure Data Explorer (not needed for local debugging).
- `ADX_TENANT_ID`: The tenant ID in which the Azure Data Explorer instance is located (if different from your default tenant).

- `WATTTIME_USER`: The username for the WattTime service.
- `WATTTIME_PASSWORD`: The password for the WattTime service.

- `DYNAMICS_ENDPOINT_URL`: The URL for the Dynamics 365 endpoint.
- `DYNAMICS_CLIENT_ID`: The client ID for authenticating with Dynamics 365.
- `DYNAMICS_CLIENT_PASSWORD`: The client secret for authenticating with Dynamics 365.
- `DYNAMICS_TENANT_ID`: The tenant ID for authenticating with Dynamics
- `DYNAMICS_ENVIRONMENT_ID`: The environment name for the Dynamics 365 instance.
- `DYNAMICS_COMPANY_NAME`: The company name for the Dynamics 365 instance.
- `DYNAMICS_PRODUCT_NAME`: The product name for the Dynamics 365 instance.
- `DYNAMICS_BATCH_NAME`: The batch name for the Dynamics 365 instance.
