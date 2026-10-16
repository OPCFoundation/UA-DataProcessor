using Microsoft.AspNetCore.WebUtilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Opc.Ua.Cloud.Client.Models;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text;

namespace Opc.Ua.Data.Processor
{
    public class PCFProcessor
    {
        // the production line whose OPC UA data is available in InfluxDB
        private const string InfluxProductionLineName = "Munich";

        // the data source is detected from the environment: when INFLUX_TOKEN is set, OPC UA data is
        // read from InfluxDB (which only holds the Munich production line), otherwise from Azure Data
        // Explorer (which holds both the Munich and the Seattle production line)
        private static bool UseInfluxDB => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("INFLUX_TOKEN"));

        private readonly ADXDataService _adxDataService = new ADXDataService();
        private readonly InfluxDataService _influxDataService = new InfluxDataService();
        private readonly DynamicsDataService _dynamicsDataService = new DynamicsDataService();
        private const string NodesetFile = "./CarbonFootprint.NodeSet2.xml";

        private readonly CloudLibraryClient _cloudLibraryClient = new CloudLibraryClient();
        private readonly string _nodesetIdentifier;
        private readonly Dictionary<string, double> _lastProcessedSerialNumber = new();

        public PCFProcessor()
        {
            if (UseInfluxDB)
            {
                _influxDataService.Connect();
            }
            else
            {
                _adxDataService.Connect();
            }

            _dynamicsDataService.Connect();

            _cloudLibraryClient.EnsureDppBaseNodesetExists();
            _nodesetIdentifier = _cloudLibraryClient.EnsureNodesetExists(NodesetFile, "CarbonFootprint", "Product Carbon Footprint Digital Product Passport");
        }

        public void Process()
        {
            // we have two production lines in the manufacturing ontologies production line simulation and they are connected like so:
            // assembly -> test -> packaging
            CalcPCFForProductionLine(InfluxProductionLineName, "48.1375", "11.575", 6);

            // the Seattle production line is only available in Azure Data Explorer
            if (!UseInfluxDB)
            {
                CalcPCFForProductionLine("Seattle", "47.609722", "-122.333056", 10);
            }
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
                    // and get the latest finished product's serial number
                    Dictionary<string, object> latestProductProduced = ADXQueryForSpecificValue("packaging", productionLineName, "Status", 2);
                    if ((latestProductProduced != null) && latestProductProduced.ContainsKey("Timestamp"))
                    {
                        Dictionary<string, object> serialNumberResult = ADXQueryForSpecificTime("packaging", productionLineName, "ProductSerialNumber", ((DateTime)latestProductProduced["Timestamp"]).ToString("yyyy-MM-dd HH:mm:ss"), idealCycleTime);
                        if (!TryGetNodeValue(serialNumberResult, out double latestSerialNumber))
                        {
                            Console.WriteLine("CalcPCFForProductionLine: no serial number found at packaging for " + productionLineName);
                            return;
                        }

                        // products finish faster than one processing cycle, so process every serial number since the last one we processed
                        double firstSerialNumber = _lastProcessedSerialNumber.TryGetValue(productionLineName, out double lastProcessed) ? lastProcessed + 1 : latestSerialNumber;
                        if (firstSerialNumber > latestSerialNumber)
                        {
                            return;
                        }

                        // we get scope 3 emissions from Dynamics as part of the Bill of Material (BoM)
                        float scope3Emissions = RetrieveScope3Emissions();

                        for (double serialNumber = firstSerialNumber; serialNumber <= latestSerialNumber; serialNumber++)
                        {
                            CalcPCFForProduct(productionLineName, serialNumber, idealCycleTime, currentCarbonIntensity.data[0].intensity.actual, scope3Emissions);
                        }

                        _lastProcessedSerialNumber[productionLineName] = latestSerialNumber;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("CalcPCFForProductionLine: " + ex.Message);
            }
        }

        private void CalcPCFForProduct(string productionLineName, double serialNumber, int idealCycleTime, float carbonIntensity, float scope3Emissions)
        {
            // check each machine for the time when the product with this serial number was in the machine and get its energy comsumption at that time
            double energyTotal = 0;
            foreach (string station in new[] { "assembly", "test", "packaging" })
            {
                Dictionary<string, object> timeItWasProduced = ADXQueryForSpecificValue(station, productionLineName, "ProductSerialNumber", serialNumber);
                if ((timeItWasProduced == null) || !timeItWasProduced.ContainsKey("Timestamp"))
                {
                    if (station == "assembly")
                    {
                        Console.WriteLine($"CalcPCFForProduct: serial number {serialNumber} not found at {station} on {productionLineName}, skipping product.");
                    }
                    else
                    {
                        // stations can mark a product with bad quality, after which the MES abandons its serial number and it never reaches the next station
                        Console.WriteLine($"CalcPCFForProduct: serial number {serialNumber} did not reach {station} on {productionLineName} (abandoned by the MES due to bad quality), no PCF created.");
                    }

                    return;
                }

                Dictionary<string, object> energy = ADXQueryForSpecificTime(station, productionLineName, "EnergyConsumption", ((DateTime)timeItWasProduced["Timestamp"]).ToString("yyyy-MM-dd HH:mm:ss"), idealCycleTime);
                if (!TryGetNodeValue(energy, out double energyValue))
                {
                    Console.WriteLine($"CalcPCFForProduct: no energy consumption found at {station} for serial number {serialNumber} on {productionLineName}, skipping product.");
                    return;
                }

                energyTotal += energyValue;
            }

            // calculate the total energy consumption for the product by summing up all the machines' energy consumptions (in Ws), divide by 3600 to get seconds and multiply by the ideal cycle time (which is in seconds)
            energyTotal = energyTotal / 3600 * idealCycleTime;

            // we set scope 1 emissions to 0
            float scope1Emissions = 0.0f;

            // finally calculate the scope 2 product carbon footprint by multiplying the full energy consumption by the current carbon intensity
            float scope2Emissions = (float)energyTotal * carbonIntensity;

            // finally calculate our PCF
            float pcf = scope1Emissions + scope2Emissions + scope3Emissions;

            // persist in Cloud Library
            PersistInCloudLibrary(productionLineName, serialNumber, pcf);
        }

        private static bool TryGetNodeValue(Dictionary<string, object> result, out double value)
        {
            value = 0;
            if ((result != null) && result.TryGetValue("OPCUANodeValue", out object nodeValue) && (nodeValue != null))
            {
                value = Convert.ToDouble(nodeValue, CultureInfo.InvariantCulture);
                return true;
            }

            return false;
        }

        private void PersistInCloudLibrary(string productionLineName, double serialNumber, float pcf)
        {
            if (_nodesetIdentifier == null)
            {
                return;
            }

            string dppName = "CarbonFootprint_" + productionLineName + "_" + serialNumber.ToString();

            Dictionary<string, string> values = new() {
                { "i=3", productionLineName + "_" + serialNumber.ToString() }, // UniqueProductIdentifier
                { "i=4", "2" },                         // Granularity (GranularityEnumeration: 2 = Item)
                { "i=5", "1.0" },                       // DppSchemaVersion
                { "i=6", "Published" },                 // DppStatus
                { "i=7", DateTime.UtcNow.ToString("o") }, // LastUpdated
                // EconomicOperatorId. The Cloud Library refuses to issue a signed
                // credential for a passport naming an operator other than its own
                // Dpp:Esdc:EconomicOperatorId, and compares the two with
                // StringComparison.Ordinal - so this string must match that setting
                // exactly, including the urn:uacl: prefix.
                { "i=8", "urn:uacl:opcfoundation" },
                { "i=9", productionLineName },          // FacilityId
                { "i=19", "GHG Protocol" },             // PCFCalculationMethod.Value
                { "i=23", pcf.ToString() },             // PCFCO2eq.Value
                { "i=31", "gCO2" }                      // PCFQuantityOfMeasureForCalculation.Value
            };

            _cloudLibraryClient.UploadPublishedValueSet(NodesetFile, _nodesetIdentifier, dppName, "Sample PCF for Digital Twin Consortium production line simulation", values);
        }

        private float RetrieveScope3Emissions()
        {
            try
            {
                string query = "Backward" + "\r\n"
                    + Environment.GetEnvironmentVariable("DYNAMICS_COMPANY_NAME") + "\r\n"
                    + Environment.GetEnvironmentVariable("DYNAMICS_PRODUCT_NAME") + "\r\n"
                    + Environment.GetEnvironmentVariable("DYNAMICS_BATCH_NAME") + "\r\n";

                Dictionary<string, object> response = _dynamicsDataService.RunQuery(query);
                if (response.ContainsKey(query) && (response[query] != null) && response[query] is DynamicsQueryResponse dynamicsResponse)
                {
                    return FindPcf(dynamicsResponse.root);
                }
                else
                {
                    return 0.0f;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("RetrieveScope3Emissions: " + ex.Message);
                return 0.0f;
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
            if (UseInfluxDB)
            {
                return InfluxQueryForSpecificValue(stationName, productionLineName, valueToQuery, desiredValue);
            }

            string query = "opcua_metadata_lkv\r\n"
                         + "| where DataSetName contains \"" + stationName + "\"\r\n"
                         + "| where DataSetName contains \"" + productionLineName + "\"\r\n"
                         + "| join kind = inner(opcua_telemetry\r\n"
                         + "    | where Name == \"" + valueToQuery + "\"\r\n"
                         + "    | where Timestamp > now(- 1h)\r\n"
                         + ") on Subject\r\n"
                         + "| distinct Timestamp, OPCUANodeValue = todouble(Value)\r\n"
                         + "| where OPCUANodeValue == " + desiredValue.ToString("R", CultureInfo.InvariantCulture) + "\r\n"
                         + "| sort by Timestamp desc";

            return _adxDataService.RunQuery(query);
        }

        private Dictionary<string, object> ADXQueryForSpecificTime(string stationName, string productionLineName, string valueToQuery, string timeToQuery, int idealCycleTime)
        {
            if (UseInfluxDB)
            {
                return InfluxQueryForSpecificTime(stationName, productionLineName, valueToQuery, timeToQuery, idealCycleTime);
            }

            string query = "opcua_metadata_lkv\r\n"
                         + "| where DataSetName contains \"" + stationName + "\"\r\n"
                         + "| where DataSetName contains \"" + productionLineName + "\"\r\n"
                         + "| join kind = inner(opcua_telemetry\r\n"
                         + "    | where Name == \"" + valueToQuery + "\"\r\n"
                         + "    | where Timestamp > now(- 1h)\r\n"
                         + ") on Subject\r\n"
                         + "| distinct Timestamp, OPCUANodeValue = todouble(Value)\r\n"
                         + "| where around(Timestamp, datetime(" + timeToQuery + "), " + idealCycleTime.ToString() + "s)\r\n"
                         + "| sort by Timestamp desc";

            return _adxDataService.RunQuery(query);
        }

        private Dictionary<string, object> InfluxQueryForSpecificValue(string stationName, string productionLineName, string valueToQuery, double desiredValue)
        {
            string query = _influxDataService.BuildSpecificValueQuery(stationName, productionLineName, valueToQuery, desiredValue);

            return NormalizeNodeValue(_influxDataService.RunQuery(query));
        }

        private Dictionary<string, object> InfluxQueryForSpecificTime(string stationName, string productionLineName, string valueToQuery, string timeToQuery, int idealCycleTime)
        {
            DateTime time = DateTime.SpecifyKind(DateTime.Parse(timeToQuery, CultureInfo.InvariantCulture), DateTimeKind.Utc);

            string query = _influxDataService.BuildValueAtTimeQuery(stationName, productionLineName, valueToQuery, time, idealCycleTime);

            return NormalizeNodeValue(_influxDataService.RunQuery(query));
        }

        /// <summary>
        /// The Azure Data Explorer queries project the OPC UA node value via todouble(), so make sure
        /// the InfluxDB results are of the same type for the callers.
        /// </summary>
        private static Dictionary<string, object> NormalizeNodeValue(Dictionary<string, object> values)
        {
            if ((values != null) && values.TryGetValue("OPCUANodeValue", out object nodeValue) && (nodeValue != null))
            {
                try
                {
                    values["OPCUANodeValue"] = Convert.ToDouble(nodeValue, CultureInfo.InvariantCulture);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("NormalizeNodeValue: " + ex.Message);
                }
            }

            return values;
        }
    }
}
