using Microsoft.AspNetCore.WebUtilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Opc.Ua.Cloud.Client.Models;
using System.Net.Http.Headers;
using System.Text;

namespace Opc.Ua.Data.Processor
{
    public class BatteryPassProcessor
    {
        private readonly ADXDataService _adxDataService = new ADXDataService();
        private readonly HttpClient _webClient = new HttpClient()
        {
            BaseAddress = new Uri(Environment.GetEnvironmentVariable("UA_CLOUD_LIBRARY_URL")),
            DefaultRequestHeaders =
            {
                Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(Environment.GetEnvironmentVariable("UA_CLOUD_LIBRARY_USERNAME") + ":" + Environment.GetEnvironmentVariable("UA_CLOUD_LIBRARY_PASSWORD"))))
            }
        };

        public BatteryPassProcessor()
        {
            _adxDataService.Connect();
        }

        public void Process()
        {
            GetDPPDataForProductionLine("muenster", 10);
        }

       private void GetDPPDataForProductionLine(string productionLineName, int idealCycleTime)
        {
            try
            {
                // get the latest idDmc of a cell (latest cell produced)
                Dictionary<string, object> latestProductProducedSerialNumber = ADXQueryForLastKnownValue("production", productionLineName, "idDmc");

                if ((latestProductProducedSerialNumber != null) && (latestProductProducedSerialNumber.Count > 0))
                {
                    
                    // get the EOL data that are sent very close timewise to the cellID
                    Dictionary<string, object> EOLData = ADXQueryForEOLData("production", productionLineName, ((DateTime)latestProductProducedSerialNumber["TelemetryTime"]).ToString("yyyy-MM-dd HH:mm:ss"), idealCycleTime);
                    
                    // persist in Cloud Library
                    PersistInCloudLibrary(productionLineName, latestProductProducedSerialNumber)

                }
                // }
            }
            catch (Exception ex)
            {
                Console.WriteLine("CalcPCFForProductionLine: " + ex.Message);
            }
        }

        private void PersistInCloudLibrary(string productionLineName, string serialNumber)
        {
            string dppName = "BatteryPassV6_" + productionLineName + "_" + serialNumber.ToString();

            // write the values to a JSON file
            Dictionary<string, string> values = new() {}

            UANameSpace nameSpace = new() {
                Title = dppName,
                License = "MIT",
                CopyrightText = "OPC Foundation",
                Description = "Sample BPP for OPCF / Fraunhofer FFB production line simulation"
            };
            nameSpace.Nodeset.NodesetXml = File.ReadAllText("./BatteryPassV6.NodeSet2.xml").Replace("BatteryPassV6", dppName);
            var url = QueryHelpers.AddQueryString(
                _webClient.BaseAddress.AbsoluteUri + "infomodel/upload",
                new Dictionary<string, string> {
                    ["overwrite"] = "true",
                    ["values"] = JsonConvert.SerializeObject(values)
                }
            );

            HttpResponseMessage response = _webClient.Send(new HttpRequestMessage(HttpMethod.Put, new Uri(url)) {
                Content = new StringContent(JsonConvert.SerializeObject(nameSpace),
                Encoding.UTF8, "application/json")
            });

            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine("Error uploading BatteryPass to Cloud Library: " + response.StatusCode.ToString());
            }
            else
            {
                Console.WriteLine("Successfully uploaded BatteryPass to Cloud Library for " + dppName);
            }
        }
       private float FindPcf(ErpNode node)
        {
            if (node.events != null)
            {
                foreach (ErpEvent erpEvent in node.events)
                {
                    if (erpEvent.productTransactions != null)
                    {
                        foreach (ErpTransaction transaction in erpEvent.productTransactions)
                        {
                            if ((transaction.details != null) && (transaction.details.First != null) && ((JProperty)transaction.details.First).Name.ToLowerInvariant() == "pcf")
                            {
                                return float.Parse(((JProperty)transaction.details.First).Value.ToString()) / transaction.quantity;
                            }
                        }
                    }
                }
            }

            if (node.next != null)
            {
                foreach (ErpNode nextNode in node.next)
                {
                    float pcf = FindPcf(nextNode);
                    if (pcf != 0.0f)
                    {
                        return pcf;
                    }
                }
            }

            // not found
            return 0.0f;
        }

        private Dictionary<string, object> ADXQueryForLastKnownValue(string stationName, string productionLineName, string valueToQuery)
        {
            string query = "opcua_metadata_lkv\r\n"
                         + "| where Name contains \"" + stationName + "\"\r\n"
                         + "| where Name contains \"" + productionLineName + "\"\r\n"
                         + "| join kind = inner(opcua_telemetry\r\n"
                         + "    | where Name == \"" + valueToQuery + "\"\r\n"
                         + "    | where Timestamp > now(- 1h)\r\n"
                         + "    | project TelemetryTime = Timestamp, DataSetWriterID, Value\r\n"
                         + ") on DataSetWriterID\r\n"
                         + "| distinct TelemetryTime, OPCUANodeValue = todouble(Value)\r\n"
                         + "| top 1 by TelemetryTime desc";

            return _adxDataService.RunQuery(query);
        }

        private Dictionary<string, object> ADXQueryForSpecificTime(string stationName, string productionLineName, string valueToQuery, string timeToQuery, int idealCycleTime)
        {
            string query = "opcua_metadata_lkv\r\n"
                         + "| where Name contains \"" + stationName + "\"\r\n"
                         + "| where Name contains \"" + productionLineName + "\"\r\n"
                         + "| join kind = inner(opcua_telemetry\r\n"
                         + "    | where Name == \"" + valueToQuery + "\"\r\n"
                         + "    | where Timestamp between (datetime(" + timeToQuery + ") - " +  idealCycleTime.ToString() + "datetime(" + timeToQuery + ") + " +  idealCycleTime.ToString() + ")\r\n"
                         + "    | project TelemetryTime = Timestamp, DataSetWriterID, Value\r\n"
                         + ") on DataSetWriterID\r\n"
                         + "| distinct TelemetryTime, NodeValue = tostring(Value)\r\n"
                         + "| sort by TelemetryTime desc";

            
            return _adxDataService.RunQuery(query);
        }

        private Dictionary<string, object> ADXQueryForEOLData(string stationName, string productionLineName, string timeToQuery, int idealCycleTime)
        {
            string query = "opcua_metadata_lkv\r\n"
                         + "| where Name contains \"" + stationName + "\"\r\n"
                         + "| where Name contains \"" + productionLineName + "\"\r\n"
                         + "| join kind = inner(opcua_telemetry\r\n"
                         + "    | where (Name == \"quantityValue\" and DataSetWriterID == 22669)\r\n" // weight
                         + "        or (Name == \"quantityValue\" and DataSetWriterID == 35057)\r\n" // height
                         + "        or (Name == \"quantityValue\" and DataSetWriterID == 35060)\r\n" // length
                         + "        or (Name == \"quantityValue\" and DataSetWriterID == 35067)\r\n" // width
                         + "    | where Timestamp between (datetime(" + timeToQuery + ") - " +  idealCycleTime.ToString() + "datetime(" + timeToQuery + ") + " +  idealCycleTime.ToString() + ")\r\n"
                         + "    | project DataSetWriterID, Value, TelemetryTime = Timestamp, VariableName = Name \r\n"
                         + ") on DataSetWriterID\r\n"
                         + "| extend NodeValue = tostring(Value)\r\n"
                         + "| extend DisplayName = case(\r\n"
                         + "        VariableName == \"quantityValue\" and DataSetWriterID == 22669, \"weight\",\r\n"
                         + "        VariableName == \"quantityValue\" and DataSetWriterID == 35057, \"height\",\r\n"
                         + "        VariableName == \"quantityValue\" and DataSetWriterID == 35060, \"length\",\r\n"
                         + "        VariableName == \"quantityValue\" and DataSetWriterID == 35067, \"width\",\r\n"
                         + "        strcat(VariableName)\r\n"
                         + "    )\r\n"
                         + "| summarize arg_max(Timestamp, *) by DisplayName\r\n"
                         + "| project DisplayName, NodeValue 

            return _adxDataService.RunQuery(query);
        }
    }
}
