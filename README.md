# UA-DataProcessor
Data Processor for the UA Cloud Initiative. Runs in a Docker container and processes data from connected data sources (see DataServices directory) and outputs the processed data as nodeset files into UA Cloud Library.

Currently, available data services are for Azure Data Explorer, InfluxDB and Dynamics365, but other data services can be added by implementing the simple IDataService interface.

## InfluxDB Data Service

The InfluxDB data service (`DataServices/InfluxDataService.cs`) reads OPC UA data from an InfluxDB 2.x instance using Flux queries. It expects the same database organization as [UA Cloud Action](https://github.com/OPCF-Members/UA-CloudAction):

- **Telemetry**: OPC UA PubSub telemetry is stored in the measurement configured via `INFLUX_MEASUREMENT` (default `opcua_pubsub`). Each OPC UA variable maps to an InfluxDB *field* (the string identifier of the OPC UA NodeId) and each point is tagged with the `datasetWriterId` that published it.
- **Metadata**: The corresponding OPC UA PubSub metadata is stored in the measurement configured via `INFLUX_METADATA_MEASUREMENT` (default `opcua_metadata`). It carries the OPC UA DataSetName in its `metaName` tag and is linked to the telemetry through the shared `datasetWriterId` tag.

This mirrors the Azure Data Explorer layout, where `opcua_telemetry` (Name, Value, Timestamp) is joined to `opcua_metadata_lkv` (DataSetName) on `Subject`, i.e. `Subject` == `datasetWriterId`, `Name` == field and `DataSetName` == `metaName`.

The service resolves a station/production line to its dataset writers via the metadata measurement and then queries the telemetry measurement for the requested variable. Results are returned in the same shape as the Azure Data Explorer service, with `Timestamp` and `OPCUANodeValue` keys.

The data source used by the PCF processor is detected from the environment: when `INFLUX_TOKEN` is set, OPC UA data is read from InfluxDB and only the **Munich** production line is processed (InfluxDB only holds that line). Otherwise Azure Data Explorer is used and both the **Munich** and the **Seattle** production line are processed.

In terms of data processors, currently a Product Carbon Footprint (PCF) processor following the Green-House Gas (GHG) Protocol calculation method is implemened.

# Required Environment Variables:
- `UA_CLOUD_LIBRARY_URL`: The URL for the UA Cloud Library instance to upload nodeset files to.
- `UA_CLOUD_LIBRARY_USERNAME`: The username for authenticating with the UA Cloud Library.
- `UA_CLOUD_LIBRARY_PASSWORD`: The password for authenticating with the UA Cloud Library.

- `ADX_HOST`: The hostname for the Azure Data Explorer instance.
- `ADX_DB`: The database name for the Azure Data Explorer instance.
- `ADX_APPLICATION_ID`: The application/client ID for authenticating with Azure Data Explorer (not needed for local debugging).
- `ADX_TENANT_ID`: The tenant ID in which the Azure Data Explorer instance is located (if different from your default tenant).

- `INFLUX_URL`: The URL for the InfluxDB instance (defaults to `http://influxdb.default.svc.cluster.local:8086`).
- `INFLUX_TOKEN`: The API token for authenticating with InfluxDB. If not set, the InfluxDB data service stays disconnected.
- `INFLUX_ORG`: The InfluxDB organization to query (defaults to `iot`).
- `INFLUX_BUCKET`: The InfluxDB bucket holding the OPC UA data (defaults to `mqtt`).
- `INFLUX_MEASUREMENT`: The measurement holding the OPC UA PubSub telemetry (defaults to `opcua_pubsub`).
- `INFLUX_METADATA_MEASUREMENT`: The measurement holding the OPC UA PubSub metadata (defaults to `opcua_metadata`).
- `INFLUX_TIMEOUT_SECONDS`: The HTTP timeout for InfluxDB queries in seconds (defaults to `120`).

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
