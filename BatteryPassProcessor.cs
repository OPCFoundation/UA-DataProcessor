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
            // we have two production lines in the manufacturing ontologies production line simulation and they are connected like so:
            // assembly -> test -> packaging
            CalcPCFForProductionLine("Munich", "48.1375", "11.575", 6);
            CalcPCFForProductionLine("Seattle", "47.609722", "-122.333056", 10);
        }

        private void CalcPCFForProductionLine(string productionLineName, string latitude, string longitude, int idealCycleTime)
        {
            try
            {
                // first of all, retrieve carbon intensity for the location of the production line
                CarbonIntensityQueryResult currentCarbonIntensity = WattTimeClient.GetCarbonIntensity(latitude, longitude).GetAwaiter().GetResult();
                if ((currentCarbonIntensity != null) && (currentCarbonIntensity.data.Length > 0))
                {
                    // check if a new product was produced (last machine in the production line, i.e. packaging, is in state 2 ("done") with a passed QA)
                    // and get the products serial number and energy consumption at that time
                    Dictionary<string, object> latestProductProduced = ADXQueryForSpecificValue("packaging", productionLineName, "Status", 2);
                    if ((latestProductProduced != null) && (latestProductProduced.Count > 0))
                    {
                        Dictionary<string, object> serialNumberResult = ADXQueryForSpecificTime("packaging", productionLineName, "ProductSerialNumber", ((DateTime)latestProductProduced["Timestamp"]).ToString("yyyy-MM-dd HH:mm:ss"), idealCycleTime);
                        double serialNumber = (double)serialNumberResult["OPCUANodeValue"];

                        Dictionary<string, object> timeItWasProducedPackaging = ADXQueryForSpecificValue("packaging", productionLineName, "ProductSerialNumber", serialNumber);
                        Dictionary<string, object> energyPackaging = ADXQueryForSpecificTime("packaging", productionLineName, "EnergyConsumption", ((DateTime)timeItWasProducedPackaging["Timestamp"]).ToString("yyyy-MM-dd HH:mm:ss"), idealCycleTime);

                        // check each other machine for the time when the product with this serial number was in the machine and get its energy comsumption at that time
                        Dictionary<string, object> timeItWasProducedTest = ADXQueryForSpecificValue("test", productionLineName, "ProductSerialNumber", serialNumber);
                        Dictionary<string, object> energyTest = ADXQueryForSpecificTime("test", productionLineName, "EnergyConsumption", ((DateTime)timeItWasProducedTest["Timestamp"]).ToString("yyyy-MM-dd HH:mm:ss"), idealCycleTime);

                        Dictionary<string, object> timeItWasProducedAssembly = ADXQueryForSpecificValue("assembly", productionLineName, "ProductSerialNumber", serialNumber);
                        Dictionary<string, object> energyAssembly = ADXQueryForSpecificTime("assembly", productionLineName, "EnergyConsumption", ((DateTime)timeItWasProducedAssembly["Timestamp"]).ToString("yyyy-MM-dd HH:mm:ss"), idealCycleTime);

                        // calculate the total energy consumption for the product by summing up all the machines' energy consumptions (in Ws), divide by 3600 to get seconds and multiply by the ideal cycle time (which is in seconds)
                        double energyTotal = ((double)energyAssembly["OPCUANodeValue"] + (double)energyTest["OPCUANodeValue"] + (double)energyPackaging["OPCUANodeValue"]) / 3600 * idealCycleTime;

                        // finally calculate the scope 2 product carbon footprint by multiplying the full energy consumption by the current carbon intensity
                        float scope2Emissions = (float)energyTotal * currentCarbonIntensity.data[0].intensity.actual;

                        // persist in Cloud Library
                        PersistInCloudLibrary(productionLineName, serialNumber, scope2Emissions);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("CalcPCFForProductionLine: " + ex.Message);
            }
        }

        private void PersistInCloudLibrary(string productionLineName, double serialNumber, float pcf)
        {
            string dppName = "BatteryPassV6_" + productionLineName + "_" + serialNumber.ToString();

            // write the values to a JSON file
            Dictionary<string, string> values = new() {
                { "i=2", productionLineName + "_" + serialNumber.ToString() }, // UniqueProductIdentifier
                { "i=3", "1.0" },                       // DppSchemaVersion
                { "i=4", "Released" },                  // DppStatus
                { "i=5", DateTime.UtcNow.ToString() },  // LastUpdate
                { "i=6", "OPC Foundation" },            // EconomicOperatorId
                { "i=11", "GHG Protocol" },             // PCFCalculationMethod
                { "i=12", pcf.ToString() },             // PCFCO2eq
                { "i=13", serialNumber.ToString() },    // PCFReferenceValueForCalculation
                { "i=14", "gCO2" },                     // PCFQuantityOfMeasureForCalculation
                { "i=15", "Production & Usage" },       // PCFLifeCyclePhase
                { "i=16", "Scope 2 & 3 Emissions" },    // ExplanatoryStatement
                { "i=21", productionLineName },         // PCFGoodsAddressHandover.CityTown
                { "i=23", DateTime.UtcNow.ToString() }  // PublicationDate
            };

            UANameSpace nameSpace = new() {
                Title = dppName,
                License = "MIT",
                CopyrightText = "OPC Foundation",
                Description = "Sample PCF for Digital Twin Consortium production line simulation"
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

        private Dictionary<string, object> ADXQueryForSpecificValue(string stationName, string productionLineName, string valueToQuery, double desiredValue)
        {
            string query = "opcua_metadata_lkv\r\n"
                         + "| where Name contains \"" + stationName + "\"\r\n"
                         + "| where Name contains \"" + productionLineName + "\"\r\n"
                         + "| join kind = inner(opcua_telemetry\r\n"
                         + "    | where Name == \"" + valueToQuery + "\"\r\n"
                         + "    | where Timestamp > now(- 1h)\r\n"
                         + ") on DataSetWriterID\r\n"
                         + "| distinct Timestamp, OPCUANodeValue = todouble(Value)\r\n"
                         + "| sort by Timestamp desc";

            return _adxDataService.RunQuery(query);
        }

        private Dictionary<string, object> ADXQueryForSpecificTime(string stationName, string productionLineName, string valueToQuery, string timeToQuery, int idealCycleTime)
        {
            string query = "opcua_metadata_lkv\r\n"
                         + "| where Name contains \"" + stationName + "\"\r\n"
                         + "| where Name contains \"" + productionLineName + "\"\r\n"
                         + "| join kind = inner(opcua_telemetry\r\n"
                         + "    | where Name == \"" + valueToQuery + "\"\r\n"
                         + "    | where Timestamp > now(- 1h)\r\n"
                         + ") on DataSetWriterID\r\n"
                         + "| distinct Timestamp, OPCUANodeValue = todouble(Value)\r\n"
                         + "| where around(Timestamp, datetime(" + timeToQuery + "), " + idealCycleTime.ToString() + "s)\r\n"
                         + "| sort by Timestamp desc";

            return _adxDataService.RunQuery(query);
        }
    }
}
