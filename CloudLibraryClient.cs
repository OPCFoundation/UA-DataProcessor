using Opc.Ua.Cloud.Client.Models;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;

namespace Opc.Ua.Data.Processor
{
    public class CloudLibraryClient
    {
        private static readonly XNamespace UANodeSetNamespace = "http://opcfoundation.org/UA/2011/03/UANodeSet.xsd";

        private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

        private readonly HttpClient _webClient = new HttpClient()
        {
            BaseAddress = new Uri(Environment.GetEnvironmentVariable("UA_CLOUD_LIBRARY_URL")),
            DefaultRequestHeaders =
            {
                Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(Environment.GetEnvironmentVariable("UA_CLOUD_LIBRARY_USERNAME") + ":" + Environment.GetEnvironmentVariable("UA_CLOUD_LIBRARY_PASSWORD"))))
            }
        };

        private record ValueSetSummary(Guid Id, string NodesetIdentifier, string Name, bool IsPublished);

        private const string DppBaseNodesetFile = "./Opc.Ua.DigitalProductPassport.NodeSet2.xml";

        // makes sure the Digital Product Passport base nodeset, which all DPP nodesets depend on, is available in the Cloud Library
        public string EnsureDppBaseNodesetExists()
        {
            return EnsureNodesetExists(DppBaseNodesetFile, "DigitalProductPassport", "OPC UA Digital Product Passport base model");
        }

        // makes sure the nodeset is available in the Cloud Library (uploading it if it isn't) and returns its identifier
        public string EnsureNodesetExists(string nodesetFile, string title, string description)
        {
            string nodesetXml = File.ReadAllText(nodesetFile);
            string namespaceUri = GetModelUri(nodesetXml);

            string identifier = FindNodesetIdentifier(namespaceUri);
            if (identifier != null)
            {
                return identifier;
            }

            UANameSpace nameSpace = new() {
                Title = title,
                License = "MIT",
                CopyrightText = "OPC Foundation",
                Description = description
            };
            nameSpace.Nodeset.NodesetXml = nodesetXml;

            HttpResponseMessage response = _webClient.Send(new HttpRequestMessage(HttpMethod.Put, "infomodel/upload?overwrite=false") {
                Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(nameSpace), Encoding.UTF8, "application/json")
            });

            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine("Error uploading nodeset " + namespaceUri + " to Cloud Library: " + response.StatusCode.ToString());
                return null;
            }

            Console.WriteLine("Successfully uploaded nodeset " + namespaceUri + " to Cloud Library");

            return FindNodesetIdentifier(namespaceUri);
        }

        // creates (or reuses) a value set for the nodeset, writes the values into it and publishes it as a DPP
        public bool UploadPublishedValueSet(string nodesetFile, string nodesetIdentifier, string name, string description, Dictionary<string, string> values)
        {
            Guid? valueSetId = CreateOrGetValueSet(nodesetIdentifier, name, description);
            if (valueSetId == null)
            {
                return false;
            }

            // values are keyed by expanded NodeId (local keys may be "i=N" or "ns=1;i=N")
            string namespaceUri = GetModelUri(File.ReadAllText(nodesetFile));
            Dictionary<string, string> expandedValues = values.ToDictionary(v => "nsu=" + namespaceUri + ";" + (v.Key.StartsWith("ns=1;") ? v.Key.Substring(5) : v.Key), v => v.Value);

            HttpResponseMessage response = _webClient.Send(new HttpRequestMessage(HttpMethod.Put, "valuesets/" + valueSetId + "/values") {
                Content = new StringContent(JsonSerializer.Serialize(new { values = expandedValues }), Encoding.UTF8, "application/json")
            });

            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine("Error writing values to value set " + name + ": " + response.StatusCode.ToString() + " " + response.Content.ReadAsStringAsync().GetAwaiter().GetResult());
                return false;
            }

            response = _webClient.Send(new HttpRequestMessage(HttpMethod.Post, "valuesets/" + valueSetId + "/publish"));
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine("Error publishing value set " + name + ": " + response.StatusCode.ToString());
                return false;
            }

            Console.WriteLine("Successfully uploaded and published value set " + name + " to Cloud Library");
            return true;
        }

        private Guid? CreateOrGetValueSet(string nodesetIdentifier, string name, string description)
        {
            HttpResponseMessage response = _webClient.Send(new HttpRequestMessage(HttpMethod.Post, "valuesets") {
                Content = new StringContent(JsonSerializer.Serialize(new { nodesetIdentifier, name, description }), Encoding.UTF8, "application/json")
            });

            if (response.StatusCode == HttpStatusCode.Conflict)
            {
                // a set with this name already exists for the nodeset, so reuse it
                response = _webClient.Send(new HttpRequestMessage(HttpMethod.Get, "valuesets?nodesetIdentifier=" + Uri.EscapeDataString(nodesetIdentifier)));
                if (response.IsSuccessStatusCode)
                {
                    ValueSetSummary[] valueSets = JsonSerializer.Deserialize<ValueSetSummary[]>(response.Content.ReadAsStringAsync().GetAwaiter().GetResult(), JsonOptions) ?? [];
                    ValueSetSummary existing = valueSets.FirstOrDefault(v => v.Name == name);
                    if (existing != null)
                    {
                        return existing.Id;
                    }
                }
            }
            else if (response.IsSuccessStatusCode)
            {
                return JsonSerializer.Deserialize<ValueSetSummary>(response.Content.ReadAsStringAsync().GetAwaiter().GetResult(), JsonOptions)?.Id;
            }

            Console.WriteLine("Error creating value set " + name + " in Cloud Library: " + response.StatusCode.ToString());
            return null;
        }

        private string FindNodesetIdentifier(string namespaceUri)
        {
            HttpResponseMessage response = _webClient.Send(new HttpRequestMessage(HttpMethod.Get, "infomodel/namespaces"));
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine("Error reading namespaces from Cloud Library: " + response.StatusCode.ToString());
                return null;
            }

            // each entry has the format "<namespaceUri>,<identifier>"
            string[] namespaces = JsonSerializer.Deserialize<string[]>(response.Content.ReadAsStringAsync().GetAwaiter().GetResult()) ?? [];
            string match = namespaces.FirstOrDefault(n => n.Split(',')[0].Equals(namespaceUri, StringComparison.Ordinal));

            return match?.Split(',').ElementAtOrDefault(1);
        }

        private static string GetModelUri(string nodesetXml)
        {
            return XDocument.Parse(nodesetXml).Root
                .Element(UANodeSetNamespace + "Models")
                .Element(UANodeSetNamespace + "Model")
                .Attribute("ModelUri").Value;
        }
    }
}
