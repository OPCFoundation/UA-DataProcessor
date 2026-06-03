# UA-DataProcessor
Data Processor for the UA Cloud Initiative. Runs in a Docker container and processes data from connected data sources (see DataServices directory) and outputs the processed data as nodeset files into UA Cloud Library.

Currently, available data services are for Azure Data Explorer and Dynamics365, but other data services can be added by implementing the simple IDataService interface.

In terms of data processors, currently a Product Carbon Footprint (PCF) processor following the Green-House Gas (GHG) Protocol calculation method is implemened.

# Required Environment Variables:
- `DATA_PROCESSOR_MODE`: Runtime mode selector. Use `default` (or leave it unset/empty) for PCF/BatteryPass processing mode, or `wss` (or `wss-bridge`) for the WSS-to-publishednodes bridge mode.

## Default mode (`DATA_PROCESSOR_MODE=default` or empty)
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

## WSS bridge mode (`DATA_PROCESSOR_MODE=wss`)
Architecture overview: this bridge connects to the Eclipse Dataspace WSS server extension and receives transfer commands over WSS; on the other side, it forwards the generated publishednodes payload to the Cloud Publisher REST API.
- `WSS_ENDPOINT`: WSS server endpoint for receiving transfer messages (for example `wss://example.com/transfer`).
- `WSS_API_KEY`: API key used by the WSS client connection.
- `WSS_CLIENT_ID`: Client identifier sent in query string and ping/ack messages (default: machine name).
- `WSS_TRANSFER_EXECUTOR`: Transfer execution strategy. Currently supported: `opcua-publishednodes` (aliases: `opcua`).
- `PUBLISHED_NODES_API_URL`: REST endpoint to receive the generated publishednodes JSON via HTTP POST. The current transfer ID is sent as `registrationId` in the query string.
- `PUBLISHED_NODES_API_BEARER_KEY`: Bearer token used for the REST API authorization header.

Optional variables for WSS bridge mode:
- `PUBLISHED_NODES_OPC_ENDPOINT_URL`: Default OPC UA endpoint URL for generated `EndpointUrl` (default: `opc.tcp://localhost:4840`).
- `PUBLISHED_NODES_USE_SECURITY`: Default `UseSecurity` value (`true`/`false`, default: `false`).
- `PUBLISHED_NODES_PUBLISHING_INTERVAL`: Default `OpcPublishingInterval` in ms for each node (default: `5000`).
- `PUBLISHED_NODES_HEARTBEAT_INTERVAL`: Default `HeartbeatInterval` in ms for each node (default: `5000`).
- `WSS_RECONNECT_INTERVAL_MS`: Reconnect backoff in milliseconds (default: `5000`).
- `WSS_PING_INTERVAL_SECONDS`: Ping interval while connected (default: `30`).
- `WSS_CONNECT_TIMEOUT_SECONDS`: Timeout for initial WebSocket connect attempt (default: `15`).

### Supported WSS message types
- `opcua_read_request`: starts/updates transfer state and posts the publishednodes payload for that transfer.
- `suspend_transfer`: deletes the transfer registration via `DELETE /api/publishing/publishednodes/{registrationKey}`.
- `terminate_transfer`: deletes the transfer registration via `DELETE /api/publishing/publishednodes/{registrationKey}`.
- `ping`: responds with `pong`.
- `pong`, `welcome`, `error`, `ack`, `subscribed`, `unsubscribed`, `response`, `transfer_ack`: intentionally ignored.

For transfer lifecycle commands, the client responds with `transfer_ack` messages using statuses `started`, `suspended`, `terminated`, or `failed`.

### Internal component split (WSS mode)
- `IMessageConnection`: generic transport contract for receiving text messages, sending text messages, and owning connection lifecycle.
- `WssClientConnection`: WSS-specific `IMessageConnection` implementation that owns WebSocket connect/reconnect lifecycle, auth/query params, receive loop, and periodic ping.
- `ITransferLifecycleHandler`: generic protocol/message handling contract that reacts to incoming transport messages.
- `WssTransferLifecycleHandler`: WSS transfer-lifecycle implementation that parses protocol message types, responds to `ping`, sends `transfer_ack`, and delegates command execution.
- `IDataSpaceTransferExecutor`: generic application-layer contract for transfer start/suspend/terminate/untyped behaviors.
- `OpcUaPublishedNodesTransferService`: OPC UA-specific executor that converts node lists to publishednodes JSON and posts to the configured REST endpoint.
- `WssPublishedNodesBridge`: WSS composition root that wires `IMessageConnection` + `ITransferLifecycleHandler` + selected `IDataSpaceTransferExecutor` from environment variables, with constructor injection support for reuse and testing.

To support other dataspace scenarios, add another `IDataSpaceTransferExecutor` implementation and select it via `WSS_TRANSFER_EXECUTOR`, or compose a different `IMessageConnection` implementation if the transport is not WSS.

### Example WSS Message Format (EDC Industrial)

The executor processes messages like:

```json
{
  "type": "opcua_read_request",
  "transferId": "a8cd1bca-0928-43c1-98ae-18ea23111daa",
  "nodeIds": [
	"i=2259, i=2258, ns=14;i=58250, ns=14;i=58259, ns=14;i=58255, ns=14;i=58260, i=2267"
  ],
  "mqttTopic": "opcuamqtt-machine-tool-qna",
  "pushInterval": 5000,
  "timestamp": "2026-05-28T12:40:30.523592Z"
}
```

- `type`: command type (`opcua_read_request`, `suspend_transfer`, `terminate_transfer`, `ping`)
- `transferId`: unique transfer identifier
- `nodeIds`: array of comma-separated OPC UA node ID strings (parsed and split by executor)
- `pushInterval`: interval in ms for publishing and heartbeat (5000 ms = 5 seconds)
- `mqttTopic`: target MQTT topic (optional, for reference)
- `timestamp`: ISO8601 timestamp

This feature was implemented by VDMA.
