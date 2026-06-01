using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using UA_DataProcessor.Interfaces;

namespace UA_DataProcessor
{
    public class WssTransferLifecycleHandler : ITransferLifecycleHandler
    {
        private readonly IDataSpaceTransferService _service;
        private readonly string _clientId;

        private static readonly HashSet<string> SilentTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "pong", "welcome", "error", "ack", "subscribed", "unsubscribed", "response", "transfer_ack"
        };

        public WssTransferLifecycleHandler(IDataSpaceTransferService service, string clientId)
        {
            _service = service;
            _clientId = clientId;
        }

        public async Task HandleMessageAsync(IMessageConnection connection, string rawMessage, CancellationToken cancellationToken)
        {
            try
            {
                JObject payload = JObject.Parse(rawMessage);
                string type = payload.TryGetValue("type", StringComparison.OrdinalIgnoreCase, out JToken typeToken)
                    ? typeToken.ToString().Trim().ToLowerInvariant()
                    : string.Empty;

                switch (type)
                {
                    case "opcua_read_request":
                        await ExecuteStartTransferAsync(connection, payload, cancellationToken).ConfigureAwait(false);
                        return;

                    case "suspend_transfer":
                        await ExecuteSuspendTransferAsync(connection, payload, cancellationToken).ConfigureAwait(false);
                        return;

                    case "terminate_transfer":
                        await ExecuteTerminateTransferAsync(connection, payload, cancellationToken).ConfigureAwait(false);
                        return;

                    case "ping":
                        await SendPongAsync(connection, cancellationToken).ConfigureAwait(false);
                        return;

                    default:
                        if (!string.IsNullOrWhiteSpace(type) && !SilentTypes.Contains(type))
                        {
                            Console.WriteLine("Ignoring unknown message type: " + type);
                            return;
                        }

                        if (string.IsNullOrWhiteSpace(type))
                        {
                            await _service.ExecuteUntypedAsync(payload, cancellationToken).ConfigureAwait(false);
                        }

                        return;
                }
            }
            catch (JsonReaderException)
            {
                Console.WriteLine("Ignoring non-JSON WebSocket message.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to process WSS message: " + ex.Message);
            }
        }

        private async Task ExecuteStartTransferAsync(IMessageConnection connection, JObject payload, CancellationToken cancellationToken)
        {
            string transferId = payload["transferId"]?.ToString() ?? payload["transfer_id"]?.ToString() ?? Guid.NewGuid().ToString("N");
            (string ackStatus, string message) result = await _service.StartTransferAsync(transferId, payload, cancellationToken).ConfigureAwait(false);
            await SendTransferAckAsync(connection, transferId, result.ackStatus, cancellationToken).ConfigureAwait(false);
            Console.WriteLine(result.message);
        }

        private async Task ExecuteSuspendTransferAsync(IMessageConnection connection, JObject payload, CancellationToken cancellationToken)
        {
            string transferId = payload["transferId"]?.ToString() ?? payload["transfer_id"]?.ToString() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(transferId))
            {
                return;
            }

            (string ackStatus, string message) result = await _service.SuspendTransferAsync(transferId, payload, cancellationToken).ConfigureAwait(false);
            await SendTransferAckAsync(connection, transferId, result.ackStatus, cancellationToken).ConfigureAwait(false);
            Console.WriteLine(result.message);
        }

        private async Task ExecuteTerminateTransferAsync(IMessageConnection connection, JObject payload, CancellationToken cancellationToken)
        {
            string transferId = payload["transferId"]?.ToString() ?? payload["transfer_id"]?.ToString() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(transferId))
            {
                return;
            }

            (string ackStatus, string message) result = await _service.TerminateTransferAsync(transferId, payload, cancellationToken).ConfigureAwait(false);
            await SendTransferAckAsync(connection, transferId, result.ackStatus, cancellationToken).ConfigureAwait(false);
            Console.WriteLine(result.message);
        }

        private async Task SendPongAsync(IMessageConnection connection, CancellationToken cancellationToken)
        {
            JObject payload = new JObject
            {
                ["type"] = "pong",
                ["clientId"] = _clientId,
                ["timestamp"] = DateTimeOffset.UtcNow.ToString("o")
            };

            await connection.SendTextAsync(payload.ToString(Formatting.None), cancellationToken).ConfigureAwait(false);
        }

        private async Task SendTransferAckAsync(IMessageConnection connection, string transferId, string status, CancellationToken cancellationToken)
        {
            JObject payload = new JObject
            {
                ["type"] = "transfer_ack",
                ["transferId"] = transferId,
                ["status"] = status,
                ["clientId"] = _clientId,
                ["timestamp"] = DateTimeOffset.UtcNow.ToString("o")
            };

            await connection.SendTextAsync(payload.ToString(Formatting.None), cancellationToken).ConfigureAwait(false);
        }

    }
}

