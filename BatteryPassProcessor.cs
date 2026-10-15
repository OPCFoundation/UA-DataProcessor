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
        private const string NodesetFile = "./BatteryPassV6.NodeSet2.xml";

        private readonly CloudLibraryClient _cloudLibraryClient = new CloudLibraryClient();
        private readonly string _nodesetIdentifier;

        public BatteryPassProcessor()
        {
            _adxDataService.Connect();

            _cloudLibraryClient.EnsureDppBaseNodesetExists();
            _nodesetIdentifier = _cloudLibraryClient.EnsureNodesetExists(NodesetFile, "BatteryPassV6", "Battery Passport V6 Digital Product Passport");
        }

        public void Process()
        {
            GetDPPDataForProductionLine("muenster", 60);
        }

        private void GetDPPDataForProductionLine(string productionLineName, int idealCycleTime)
        {
            try
            {
                // get the latest idDmc of a cell (latest cell produced)
                Dictionary<string, object> latestProductProduced = ADXQueryForLastKnownValue("production", productionLineName, "idDmc");

                if ((latestProductProduced != null) && (latestProductProduced.Count > 0))
                {
                    // get the EOL data that are sent very close timewise to the cellID
                    Dictionary<string, object> eolData = ADXQueryForEOLData("production", productionLineName, ((DateTime)latestProductProduced["TelemetryTime"]).ToString("yyyy-MM-dd HH:mm:ss"), idealCycleTime);

                    // persist in Cloud Library
                    PersistInCloudLibrary(productionLineName, double.ConvertToInteger<System.Numerics.BigInteger>(double.Parse(latestProductProduced["OPCUANodeValue"].ToString())).ToString(), eolData);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("CalcPCFForProductionLine: " + ex.Message);
            }
        }

        private void PersistInCloudLibrary(string productionLineName, string serialNumber, Dictionary<string, object> eolData)
        {
            if (_nodesetIdentifier == null)
            {
                return;
            }

            string dppName = "BatteryPassV6_" + productionLineName + "_" + serialNumber;

            Dictionary<string, string> values = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText("./BatteryPassV6Values.json"));
            values["ns=1;i=5"] = "1.0";                                       // DppSchemaVersion
            values["ns=1;i=6"] = "Published";                                 // DppStatus
            values["ns=1;i=7"] = DateTime.UtcNow.ToString("o"); // LastUpdated
            // EconomicOperatorId. Set here rather than relying on the template file:
            // the Cloud Library refuses to issue a signed credential for a passport
            // naming an operator other than its own Dpp:Esdc:EconomicOperatorId, and
            // compares the two with StringComparison.Ordinal. Keeping it in code
            // means it cannot drift out of sync with a stale template.
            values["ns=1;i=8"] = "urn:uacl:opcfoundation";
            values["ns=1;i=3"] = serialNumber;                                // UniqueProductIdentifier
            values["ns=1;i=26"] = serialNumber;                               // identification.idDmc
            values["ns=1;i=45"] = serialNumber.Substring(0, 19);              // identification.serialInformation.value
            values["ns=1;i=58"] = serialNumber.Substring(0, 14) + "001";      // identification.batchInformation.value
            values["ns=1;i=153"] = eolData.Values.ElementAt(1).ToString();    // physicalDimension.height
            values["ns=1;i=163"] = eolData.Values.ElementAt(3).ToString();    // physicalDimension.length
            values["ns=1;i=173"] = eolData.Values.ElementAt(5).ToString();    // physicalDimension.width
            values["ns=1;i=183"] = eolData.Values.ElementAt(7).ToString();    // physicalDimension.weight

            _cloudLibraryClient.UploadPublishedValueSet(NodesetFile, _nodesetIdentifier, dppName, "Sample BPP for OPCF / Fraunhofer FFB production line simulation", values);
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
                         + "| where DataSetName contains \"" + stationName + "\"\r\n"
                         + "| where DataSetName contains \"" + productionLineName + "\"\r\n"
                         + "| join kind = inner(opcua_telemetry\r\n"
                         + "    | where Name == \"" + valueToQuery + "\"\r\n"
                         + "    | where Timestamp > now(- 1h)\r\n"
                         + "    | project TelemetryTime = Timestamp, Subject, Value\r\n"
                         + ") on Subject\r\n"
                         + "| distinct TelemetryTime, OPCUANodeValue = todouble(Value)\r\n"
                         + "| top 1 by TelemetryTime desc";

            return _adxDataService.RunQuery(query);
        }

        private Dictionary<string, object> ADXQueryForEOLData(string stationName, string productionLineName, string timeToQuery, int idealCycleTime)
        {
            string query = "opcua_metadata_lkv\r\n"
                         + "| where DataSetName contains \"" + stationName + "\"\r\n"
                         + "| where DataSetName contains \"" + productionLineName + "\"\r\n"
                         + "| join kind = inner(opcua_telemetry\r\n"
                         + "    | where (Name == \"quantityValue\" and Subject == \"22669\")\r\n" // weight
                         + "        or (Name == \"quantityValue\" and Subject == \"35057\")\r\n" // height
                         + "        or (Name == \"quantityValue\" and Subject == \"35060\")\r\n" // length
                         + "        or (Name == \"quantityValue\" and Subject == \"35067\")\r\n" // width
                         + "    | where Timestamp between (datetime(" + timeToQuery + ") - " + idealCycleTime.ToString() + "s .. datetime(" + timeToQuery + ") + " + idealCycleTime.ToString() + "s)\r\n"
                         + "    | project Subject, Value, TelemetryTime = Timestamp, VariableName = Name \r\n"
                         + ") on Subject\r\n"
                         + "| extend NodeValue = tostring(Value)\r\n"
                         + "| extend DisplayName = case(\r\n"
                         + "        VariableName == \"quantityValue\" and Subject == \"22669\", \"weight\",\r\n"
                         + "        VariableName == \"quantityValue\" and Subject == \"35057\", \"height\",\r\n"
                         + "        VariableName == \"quantityValue\" and Subject == \"35060\", \"length\",\r\n"
                         + "        VariableName == \"quantityValue\" and Subject == \"35067\", \"width\",\r\n"
                         + "        strcat(VariableName)\r\n"
                         + "    )\r\n"
                         + "| summarize arg_max(Timestamp, *) by DisplayName\r\n"
                         + "| project DisplayName, NodeValue";

            return _adxDataService.RunQuery(query, true);
        }
    }
}
