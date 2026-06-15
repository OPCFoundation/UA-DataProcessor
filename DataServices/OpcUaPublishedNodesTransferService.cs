using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Text;
using UA_DataProcessor.Interfaces;

namespace Opc.Ua.Data.Processor
{
    public class OpcUaPublishedNodesTransferService : IDataSpaceTransferService
    {
        private readonly HttpClient _httpClient = new HttpClient();
        private readonly string _publishedNodesApiUrl;
        private readonly string _publishedNodesApiBearerKey;
        private readonly string _defaultOpcEndpointUrl;
        private readonly bool _defaultUseSecurity;
        private readonly int _defaultPublishingInterval;
        private readonly int _defaultHeartbeatInterval;

        private readonly ConcurrentDictionary<string, TransferState> _activeTransfers = new ConcurrentDictionary<string, TransferState>(StringComparer.Ordinal);

        public OpcUaPublishedNodesTransferService(
            string publishedNodesApiUrl,
            string publishedNodesApiBearerKey,
            string defaultOpcEndpointUrl,
            bool defaultUseSecurity,
            int defaultPublishingInterval,
            int defaultHeartbeatInterval)
        {
            _publishedNodesApiUrl = publishedNodesApiUrl;
            _publishedNodesApiBearerKey = publishedNodesApiBearerKey;
            _defaultOpcEndpointUrl = defaultOpcEndpointUrl;
            _defaultUseSecurity = defaultUseSecurity;
            _defaultPublishingInterval = defaultPublishingInterval;
            _defaultHeartbeatInterval = defaultHeartbeatInterval;
        }

        public async Task<(string AckStatus, string Message)> StartTransferAsync(string transferId, JObject payload, CancellationToken cancellationToken)
        {
            string endpointUrl = ResolveEndpointUrl(payload);
            bool useSecurity = ResolveUseSecurity(payload);
            int pushInterval = ParseIntToken(payload["pushInterval"], _defaultPublishingInterval);
            int publishingInterval = ParseIntToken(payload["OpcPublishingInterval"], pushInterval);
            int heartbeatInterval = ParseIntToken(payload["HeartbeatInterval"], pushInterval);
            List<string> nodeIds = ExtractNodeIds(payload);
            string topic = payload["mqttTopic"]?.ToString() ?? string.Empty;

            if (nodeIds.Count == 0)
            {
                return ("failed", "No OPC node ids found for transfer " + transferId);
            }

            TransferState transfer = new TransferState
            {
                EndpointUrl = endpointUrl,
                UseSecurity = useSecurity,
                OpcNodes = nodeIds
                    .Distinct(StringComparer.Ordinal)
                    .Select(nodeId => new PublishedOpcNode
                    {
                        Id = nodeId,
                        OpcPublishingInterval = publishingInterval,
                        HeartbeatInterval = heartbeatInterval
                    })
                    .ToList()
            };

            _activeTransfers[transferId] = transfer;
            await PostPublishedNodesAsync(transferId, topic, new List<PublishedNodesEntry>
            {
                new PublishedNodesEntry
                {
                    EndpointUrl = transfer.EndpointUrl,
                    UseSecurity = transfer.UseSecurity,
                    OpcNodes = transfer.OpcNodes
                }
            }, cancellationToken).ConfigureAwait(false);

            return ("started", "Transfer " + transferId + " started with " + transfer.OpcNodes.Count + " node(s).");
        }

        public async Task<(string AckStatus, string Message)> SuspendTransferAsync(string transferId, JObject payload, CancellationToken cancellationToken)
        {
            _activeTransfers.TryRemove(transferId, out _);
            await DeletePublishedNodesAsync(transferId, cancellationToken).ConfigureAwait(false);
            return ("suspended", "Transfer " + transferId + " suspended.");
        }

        public async Task<(string AckStatus, string Message)> TerminateTransferAsync(string transferId, JObject payload, CancellationToken cancellationToken)
        {
            _activeTransfers.TryRemove(transferId, out _);
            await DeletePublishedNodesAsync(transferId, cancellationToken).ConfigureAwait(false);
            return ("terminated", "Transfer " + transferId + " terminated.");
        }

        public async Task ExecuteUntypedAsync(JObject payload, CancellationToken cancellationToken)
        {
            string endpointUrl = ResolveEndpointUrl(payload);
            bool useSecurity = ResolveUseSecurity(payload);
            int pushInterval = ParseIntToken(payload["pushInterval"], _defaultPublishingInterval);
            int publishingInterval = ParseIntToken(payload["OpcPublishingInterval"], pushInterval);
            int heartbeatInterval = ParseIntToken(payload["HeartbeatInterval"], pushInterval);
            List<string> nodeIds = ExtractNodeIds(payload);
            string topic = payload["mqttTopic"]?.ToString() ?? string.Empty;
            string registrationId = ResolveRegistrationId(payload);

            if (nodeIds.Count == 0)
            {
                Console.WriteLine("WSS message ignored: no OPC node ids found.");
                return;
            }

            List<PublishedNodesEntry> publishedNodes = new List<PublishedNodesEntry>
            {
                new PublishedNodesEntry
                {
                    EndpointUrl = endpointUrl,
                    UseSecurity = useSecurity,
                    OpcNodes = nodeIds
                        .Distinct(StringComparer.Ordinal)
                        .Select(nodeId => new PublishedOpcNode
                        {
                            Id = nodeId,
                            OpcPublishingInterval = publishingInterval,
                            HeartbeatInterval = heartbeatInterval
                        })
                        .ToList()
                }
            };

            await PostPublishedNodesAsync(registrationId, topic, publishedNodes, cancellationToken).ConfigureAwait(false);
            Console.WriteLine("PublishedNodes payload forwarded with " + publishedNodes[0].OpcNodes.Count + " node(s).");
        }

        private async Task PostPublishedNodesAsync(string registrationId, string topic, List<PublishedNodesEntry> publishedNodes, CancellationToken cancellationToken)
        {
            string url = _publishedNodesApiUrl;
            if (!string.IsNullOrWhiteSpace(registrationId))
            {
                url = AppendQueryParameter(url, "registrationKey", registrationId);
            }

            if (!string.IsNullOrWhiteSpace(topic))
            {
                url = AppendQueryParameter(url, "topic", topic);
            }

            string jsonPayload = JsonConvert.SerializeObject(publishedNodes, new JsonSerializerSettings
            {
                ContractResolver = new Newtonsoft.Json.Serialization.CamelCasePropertyNamesContractResolver(),
                Formatting = Formatting.Indented
            });
            using HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
            
            Console.WriteLine("Posting publishednodes to: " + url);

            try
            {
                string bearerKey = _publishedNodesApiBearerKey?.Trim() ?? string.Empty;
                if (string.IsNullOrWhiteSpace(bearerKey))
                {
                    Console.WriteLine("Warning: PUBLISHED_NODES_API_BEARER_KEY is empty or whitespace");
                }
                else
                {
                    request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerKey);
                }

                HttpResponseMessage response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    string responseBody = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                    Console.WriteLine("Error posting publishednodes JSON to " + url + ": " + response.StatusCode + ". " + responseBody);
                    throw new InvalidOperationException("Publishednodes POST failed with " + response.StatusCode + ".");
                }

                Console.WriteLine("Successfully posted publishednodes to " + url);
            }
            catch (FormatException ex) when (ex.Message.Contains("ASCII") || ex.Message.Contains("header"))
            {
                Console.WriteLine("Error: Request headers contain invalid characters. This may be due to non-ASCII characters in PUBLISHED_NODES_API_BEARER_KEY.");
                Console.WriteLine("Exception: " + ex.Message);
                throw;
            }
            catch (HttpIOException ex)
            {
                Console.WriteLine("Error: HTTP connection issue while posting to " + url);
                Console.WriteLine("Message: " + ex.Message);
                Console.WriteLine("Payload size: " + (request.Content?.Headers.ContentLength ?? -1) + " bytes");
                Console.WriteLine("Payload sample: " + (publishedNodes.Count > 0 ? "Array with " + publishedNodes.Count + " endpoint(s)" : "Empty"));
                throw;
            }
            catch (HttpRequestException ex)
            {
                Console.WriteLine("Error sending HTTP request to " + url);
                Console.WriteLine("Message: " + ex.Message);
                if (ex.InnerException != null)
                {
                    Console.WriteLine("Inner Exception: " + ex.InnerException.GetType().Name + " - " + ex.InnerException.Message);
                    if (ex.InnerException.InnerException != null)
                    {
                        Console.WriteLine("Root Cause: " + ex.InnerException.InnerException.GetType().Name + " - " + ex.InnerException.InnerException.Message);
                    }
                }
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unexpected error while posting publishednodes: " + ex.GetType().Name);
                Console.WriteLine("Message: " + ex.Message);
                if (ex.InnerException != null)
                {
                    Console.WriteLine("Inner Exception: " + ex.InnerException.Message);
                }
                throw;
            }
        }

        private async Task DeletePublishedNodesAsync(string registrationKey, CancellationToken cancellationToken)
        {
            string url = BuildDeleteUrl(registrationKey);
            using HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Delete, url);

            Console.WriteLine("Deleting publishednodes registration at: " + url);

            try
            {
                string bearerKey = _publishedNodesApiBearerKey?.Trim() ?? string.Empty;
                if (string.IsNullOrWhiteSpace(bearerKey))
                {
                    Console.WriteLine("Warning: PUBLISHED_NODES_API_BEARER_KEY is empty or whitespace");
                }
                else
                {
                    request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerKey);
                }

                HttpResponseMessage response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    string responseBody = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                    Console.WriteLine("Error deleting publishednodes registration from " + url + ": " + response.StatusCode + ". " + responseBody);
                    throw new InvalidOperationException("Publishednodes DELETE failed with " + response.StatusCode + ".");
                }

                Console.WriteLine("Successfully deleted publishednodes registration at " + url);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unexpected error while deleting publishednodes: " + ex.GetType().Name);
                Console.WriteLine("Message: " + ex.Message);
                throw;
            }
        }

        private string BuildDeleteUrl(string registrationKey)
        {
            UriBuilder builder = new UriBuilder(_publishedNodesApiUrl);
            string basePath = builder.Path.TrimEnd('/');
            builder.Path = basePath + "/" + Uri.EscapeDataString(registrationKey);
            builder.Query = string.Empty;
            return builder.Uri.ToString();
        }

        private static string AppendQueryParameter(string url, string name, string value)
        {
            char separator = url.Contains("?") ? '&' : '?';
            return url + separator + Uri.EscapeDataString(name) + "=" + Uri.EscapeDataString(value);
        }

        private static string ResolveRegistrationId(JObject payload)
        {
            return payload["transferId"]?.ToString()
                ?? payload["registrationId"]?.ToString()
                ?? payload["registrationKey"]?.ToString()
                ?? string.Empty;
        }

        private string ResolveEndpointUrl(JObject payload)
        {
            return payload["opcuaServer"]?.ToString()
                ?? payload["opcUaServer"]?.ToString()
                ?? payload["OpcUaServer"]?.ToString()
                ?? payload["EndpointUrl"]?.ToString()
                ?? _defaultOpcEndpointUrl;
        }

        private bool ResolveUseSecurity(JObject payload)
        {
            JToken useSecurityToken = payload["useSecurity"]
                ?? payload["UseSecurity"]
                ?? payload["securityEnabled"]
                ?? payload["SecurityEnabled"];

            return ParseBoolToken(useSecurityToken, _defaultUseSecurity);
        }

        private static int ParseIntToken(JToken token, int defaultValue)
        {
            if (token == null)
            {
                return defaultValue;
            }

            if (token.Type == JTokenType.Integer)
            {
                return token.Value<int>();
            }

            if (int.TryParse(token.ToString(), out int parsed))
            {
                return parsed;
            }

            return defaultValue;
        }

        private static bool ParseBoolToken(JToken token, bool defaultValue)
        {
            if (token == null)
            {
                return defaultValue;
            }

            if (token.Type == JTokenType.Boolean)
            {
                return token.Value<bool>();
            }

            if (bool.TryParse(token.ToString(), out bool parsed))
            {
                return parsed;
            }

            return defaultValue;
        }

        private static List<string> ExtractNodeIds(JToken payload)
        {
            List<string> nodes = new List<string>();
            JToken nodeIdsToken = payload["nodeIds"];
            if (nodeIdsToken is JArray nodeIdsArray)
            {
                foreach (JToken item in nodeIdsArray)
                {
                    if (item.Type == JTokenType.String)
                    {
                        string content = item.ToString();
                        foreach (string nodeId in content.Split(','))
                        {
                            string trimmed = nodeId.Trim();
                            if (IsOpcNodeId(trimmed))
                            {
                                nodes.Add(trimmed);
                            }
                        }
                    }
                }
                return nodes;
            }
            CollectNodeIds(payload, nodes);
            return nodes.Where(IsOpcNodeId).ToList();
        }

        private static void CollectNodeIds(JToken token, List<string> nodeIds)
        {
            if (token is JArray array)
            {
                foreach (JToken item in array)
                {
                    if (item.Type == JTokenType.String && IsOpcNodeId(item.ToString()))
                    {
                        nodeIds.Add(item.ToString());
                    }
                    else
                    {
                        CollectNodeIds(item, nodeIds);
                    }
                }

                return;
            }

            if (token is not JObject obj)
            {
                return;
            }

            foreach (JProperty property in obj.Properties())
            {
                if (property.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)
                    || property.Name.Equals("NodeId", StringComparison.OrdinalIgnoreCase)
                    || property.Name.Equals("Identifier", StringComparison.OrdinalIgnoreCase))
                {
                    if (property.Value.Type == JTokenType.String && IsOpcNodeId(property.Value.ToString()))
                    {
                        nodeIds.Add(property.Value.ToString());
                    }
                }

                if (property.Name.Equals("NodeSetList", StringComparison.OrdinalIgnoreCase)
                    || property.Name.Equals("NodesetList", StringComparison.OrdinalIgnoreCase)
                    || property.Name.Equals("OpcNodes", StringComparison.OrdinalIgnoreCase)
                    || property.Name.Equals("NodeIds", StringComparison.OrdinalIgnoreCase)
                    || property.Name.Equals("Nodes", StringComparison.OrdinalIgnoreCase))
                {
                    CollectNodeIds(property.Value, nodeIds);
                }
                else if (property.Value is JContainer)
                {
                    CollectNodeIds(property.Value, nodeIds);
                }
            }
        }

        private static bool IsOpcNodeId(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            string normalized = value.Trim();
            return normalized.StartsWith("i=", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("s=", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("g=", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("b=", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("ns=", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("nsu=", StringComparison.OrdinalIgnoreCase);
        }

        private sealed class PublishedNodesEntry
        {
            public string EndpointUrl { get; set; } = string.Empty;
            public bool UseSecurity { get; set; }
            public List<PublishedOpcNode> OpcNodes { get; set; } = new List<PublishedOpcNode>();
        }

        private sealed class PublishedOpcNode
        {
            public string Id { get; set; } = string.Empty;
            public int OpcPublishingInterval { get; set; }
            public int HeartbeatInterval { get; set; }
        }

        private sealed class TransferState
        {
            public string EndpointUrl { get; set; } = string.Empty;
            public bool UseSecurity { get; set; }
            public List<PublishedOpcNode> OpcNodes { get; set; } = new List<PublishedOpcNode>();
        }
    }
}
