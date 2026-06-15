
using Opc.Ua.Data.Processor;
using UA_DataProcessor.Connectors;
using UA_DataProcessor.Interfaces;

namespace UA_DataProcessor
{
    public class DataspaceWssProcessor
    {
        private readonly IMessageConnection _connection;
        private readonly ITransferLifecycleHandler _transferHandler;

        public DataspaceWssProcessor()
            : this(CreateConnectionFromEnvironment(), CreateTransferHandlerFromEnvironment())
        {
        }

        public DataspaceWssProcessor(IMessageConnection connection, ITransferLifecycleHandler transferHandler)
        {
            _connection = connection;
            _transferHandler = transferHandler;
        }

		private static IMessageConnection CreateConnectionFromEnvironment()
		{
			string wssEndpoint = GetRequiredEnvironmentVariable("WSS_ENDPOINT");
			string wssApiKey = GetOptionalEnvironmentVariable("WSS_API_KEY");
			string wssClientId = Environment.GetEnvironmentVariable("WSS_CLIENT_ID") ?? Environment.MachineName;

            return new WssClientConnection(
                new Uri(wssEndpoint),
                wssApiKey,
                wssClientId,
                ParseIntEnvironmentVariable("WSS_RECONNECT_INTERVAL_MS", 5000),
                ParseIntEnvironmentVariable("WSS_PING_INTERVAL_SECONDS", 30),
                ParseIntEnvironmentVariable("WSS_CONNECT_TIMEOUT_SECONDS", 15));
        }

        private static ITransferLifecycleHandler CreateTransferHandlerFromEnvironment()
        {
            string wssClientId = Environment.GetEnvironmentVariable("WSS_CLIENT_ID") ?? Environment.MachineName;
            IDataSpaceTransferService service = CreateExecutor();
            return new WssTransferLifecycleHandler(service, wssClientId);
        }

        private static IDataSpaceTransferService CreateExecutor()
        {
            string executorType = (Environment.GetEnvironmentVariable("WSS_TRANSFER_EXECUTOR") ?? "opcua-publishednodes").Trim().ToLowerInvariant();

            switch (executorType)
            {
                case "opcua-publishednodes":
                case "opcua":
                    return new OpcUaPublishedNodesTransferService(
                        GetRequiredEnvironmentVariable("PUBLISHED_NODES_API_URL"),
                        GetRequiredEnvironmentVariable("PUBLISHED_NODES_API_BEARER_KEY"),
                        Environment.GetEnvironmentVariable("PUBLISHED_NODES_OPC_ENDPOINT_URL") ?? "opc.tcp://localhost:4840",
                        ParseBoolEnvironmentVariable("PUBLISHED_NODES_USE_SECURITY", false),
                        ParseIntEnvironmentVariable("PUBLISHED_NODES_PUBLISHING_INTERVAL", 5000),
                        ParseIntEnvironmentVariable("PUBLISHED_NODES_HEARTBEAT_INTERVAL", 5000));

                default:
                    throw new InvalidOperationException("Unsupported WSS transfer service: " + executorType);
            }
        }

        public Task RunAsync(CancellationToken cancellationToken)
        {
            return _connection.RunAsync(_transferHandler.HandleMessageAsync, cancellationToken);
        }

        public void Close()
        {
            _connection.Close();
        }

		private static string GetRequiredEnvironmentVariable(string key)
		{
			string value = Environment.GetEnvironmentVariable(key);

			return value;
		}

		private static string GetOptionalEnvironmentVariable(string key)
		{
			return Environment.GetEnvironmentVariable(key);
		}

        private static int ParseIntEnvironmentVariable(string key, int defaultValue)
        {
            string raw = Environment.GetEnvironmentVariable(key);
            if (int.TryParse(raw, out int parsed))
            {
                return parsed;
            }

            return defaultValue;
        }

        private static bool ParseBoolEnvironmentVariable(string key, bool defaultValue)
        {
            string raw = Environment.GetEnvironmentVariable(key);
            if (bool.TryParse(raw, out bool parsed))
            {
                return parsed;
            }

            return defaultValue;
        }
    }
}
