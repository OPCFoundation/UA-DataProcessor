using Microsoft.Identity.Client;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using UA_DataProcessor.Interfaces;

namespace Opc.Ua.Data.Processor
{
    public class DynamicsDataService : IDataService
    {
        private HttpClient _client = null;

        private string _instanceEndpoint = string.Empty;
        private string _clientId = string.Empty;
        private string _clientPassword = string.Empty;
        private string _tenantId = string.Empty;
        private string _environmentId = string.Empty;

        public void Connect()
        {
            _client = new();

            _instanceEndpoint = Environment.GetEnvironmentVariable("DYNAMICS_ENDPOINT_URL");
            _clientId = Environment.GetEnvironmentVariable("DYNAMICS_CLIENT_ID");
            _clientPassword = Environment.GetEnvironmentVariable("DYNAMICS_CLIENT_PASSWORD");
            _tenantId = Environment.GetEnvironmentVariable("DYNAMICS_TENANT_ID");
            _environmentId = Environment.GetEnvironmentVariable("DYNAMICS_ENVIRONMENT_ID");

            if (!string.IsNullOrEmpty(_instanceEndpoint))
            {
                Authorize().GetAwaiter().GetResult();
            }
        }

        public void Dispose()
        {
            if (_client != null)
            {
                _client.Dispose();
                _client = null;
            }
        }

        private async Task Authorize()
        {
            try
            {
                // Step 1: Get Entra token
                IConfidentialClientApplication app = ConfidentialClientApplicationBuilder.Create(_clientId)
                    .WithClientSecret(_clientPassword)
                    .WithAuthority(new Uri($"https://login.microsoftonline.com/{_tenantId}/oauth2/v2.0/token"))
                    .Build();

                AuthenticationResult result = await app.AcquireTokenForClient(["0cdb527f-a8d1-4bf8-9436-b352c68682b2/.default"]).ExecuteAsync().ConfigureAwait(false);
                Debug.WriteLine($"Token: {result.AccessToken}");

                // Step 2: Get bearer token
                HttpResponseMessage response = _client.Send(
                    new HttpRequestMessage(
                        HttpMethod.Post,
                        "https://securityservice.operations365.dynamics.com/token") {
                            Content = new StringContent(
                                "{ \"grant_type\": \"client_credentials\", \"client_assertion_type\": \"aad_app\", \"client_assertion\": \"" + result.AccessToken + "\", \"scope\": \"https://traceabilityservice.operations365.dynamics.com/.default\", \"context\": \"" + _environmentId + "\", \"context_type\": \"finops-env\" }",
                                Encoding.UTF8,
                                "application/json"
                            )
                        }
                );

                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new Exception(response.StatusCode.ToString());
                }

                DynamicsBearerTokenResponse tokenResponse = JsonConvert.DeserializeObject<DynamicsBearerTokenResponse>(await response.Content.ReadAsStringAsync().ConfigureAwait(false));

                _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenResponse.accessToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Authorize: " + ex.Message);
            }
        }

        public Dictionary<string, object> RunQuery(string query)
        {
            Dictionary<string, object> results = new();

            string[] queryLines = query.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
            DynamicsQuery queryRequest = new()
            {
                tracingDirection = queryLines.Length > 0 ? queryLines[0] : string.Empty,
                company = queryLines.Length > 1 ? queryLines[1] : string.Empty,
                itemNumber = queryLines.Length > 2 ? queryLines[2] : string.Empty,
                batchNumber = queryLines.Length > 3 ? queryLines[3] : string.Empty,
                serialNumber = queryLines.Length > 4 ? queryLines[4] : string.Empty,
                shouldIncludeEvents = true
            };

            if (!string.IsNullOrEmpty(_instanceEndpoint))
            {
                try
                {
                    string url = _instanceEndpoint + "/api/environments/" + _environmentId + "/traces/Query";
                    HttpResponseMessage response = _client.Send(
                        new HttpRequestMessage(
                            HttpMethod.Post,
                            url
                            )
                        {
                            Content = new StringContent(
                                    JsonConvert.SerializeObject(query),
                                    Encoding.UTF8,
                                    "application/json"
                            )
                        }
                    );

                    if ((response.StatusCode == HttpStatusCode.Unauthorized) || (response.StatusCode == HttpStatusCode.Forbidden))
                    {
                        Debug.WriteLine("Bearer Token expired! Attempting to retrieve a new barer token.");

                        // re-authorize
                        Authorize().GetAwaiter().GetResult();

                        // re-try our data request, using the updated bearer token
                        response = _client.Send(
                            new HttpRequestMessage(
                                HttpMethod.Post,
                                _instanceEndpoint + "/api/environment/" + _environmentId + "/traces/Query")
                            {
                                Content = new StringContent(
                                        JsonConvert.SerializeObject(query),
                                        Encoding.UTF8,
                                        "application/json"
                                )
                            }
                        );
                    }

                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        throw new Exception(response.StatusCode.ToString());
                    }

                    results.Add(query, JsonConvert.DeserializeObject<DynamicsQueryResponse>(response.Content.ReadAsStringAsync().GetAwaiter().GetResult()));
                }
                catch (Exception ex)
                {
                    Console.WriteLine("RunDynamicsQuery: " + ex.Message);
                }
            }

            return results;
        }
    }
}
