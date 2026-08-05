
using InfluxDB.Client;
using InfluxDB.Client.Core.Flux.Domain;
using System.Diagnostics;
using System.Globalization;
using UA_DataProcessor.Interfaces;

namespace Opc.Ua.Data.Processor
{
    /// <summary>
    /// InfluxDB (Flux) backed data service.
    ///
    /// The InfluxDB database is organized the same way as in UA Cloud Action:
    /// - OPC UA PubSub telemetry is written to the measurement configured via INFLUX_MEASUREMENT
    ///   (default "opcua_pubsub"). Each OPC UA variable maps to an Influx field (the string
    ///   identifier of the OPC UA NodeId), tagged with the "datasetWriterId" it was published by.
    /// - The corresponding OPC UA PubSub metadata is written to the measurement configured via
    ///   INFLUX_METADATA_MEASUREMENT (default "opcua_metadata"). It carries the OPC UA DataSetName
    ///   in its "metaName" tag and is linked to the telemetry via the shared "datasetWriterId" tag.
    ///
    /// This mirrors the ADX layout, where opcua_telemetry (Name, Value, Timestamp) is joined to
    /// opcua_metadata_lkv (DataSetName) on Subject: Subject == datasetWriterId, Name == field and
    /// DataSetName == metaName.
    /// </summary>
    public class InfluxDataService : IDataService
    {
        private InfluxDBClient _influxClient = null;

        private static string Org => Environment.GetEnvironmentVariable("INFLUX_ORG") ?? "iot";

        private static string Bucket => Environment.GetEnvironmentVariable("INFLUX_BUCKET") ?? "mqtt";

        private static string Measurement => Environment.GetEnvironmentVariable("INFLUX_MEASUREMENT") ?? "opcua_pubsub";

        private static string MetadataMeasurement => Environment.GetEnvironmentVariable("INFLUX_METADATA_MEASUREMENT") ?? "opcua_metadata";

        public void Connect()
        {
            string url = Environment.GetEnvironmentVariable("INFLUX_URL") ?? "http://influxdb.default.svc.cluster.local:8086";
            string token = Environment.GetEnvironmentVariable("INFLUX_TOKEN");

            if (string.IsNullOrEmpty(token))
            {
                Console.WriteLine("InfluxDB connection not configured (INFLUX_TOKEN missing).");
                return;
            }

            // The default HTTP timeout is only 10 seconds, which queries that scan many days of data
            // regularly exceed. Exceeding it aborts the socket read and surfaces as a
            // TaskCanceledException, so use a longer, configurable timeout instead.
            int timeoutSeconds = int.TryParse(Environment.GetEnvironmentVariable("INFLUX_TIMEOUT_SECONDS"), out int parsed) && (parsed > 0)
                ? parsed
                : 120;

            InfluxDBClientOptions options = new InfluxDBClientOptions.Builder()
                .Url(url)
                .AuthenticateToken(token)
                .TimeOut(TimeSpan.FromSeconds(timeoutSeconds))
                .Build();

            _influxClient = new InfluxDBClient(options);
        }

        public void Dispose()
        {
            if (_influxClient != null)
            {
                _influxClient.Dispose();
                _influxClient = null;
            }
        }

        /// <summary>
        /// Runs a Flux query. When <paramref name="multiRow"/> is false, the columns of the last
        /// record are returned (with "_time" surfaced as "Timestamp" and "_value" as
        /// "OPCUANodeValue" to match the ADX projections); otherwise one entry per record is
        /// returned, keyed by the record's field name.
        /// </summary>
        public Dictionary<string, object> RunQuery(string query, bool multiRow = false)
        {
            Dictionary<string, object> values = new();

            try
            {
                if (_influxClient != null)
                {
                    List<FluxTable> tables = _influxClient.GetQueryApi().QueryAsync(query, Org).GetAwaiter().GetResult();

                    foreach (FluxTable table in tables)
                    {
                        foreach (FluxRecord record in table.Records)
                        {
                            try
                            {
                                if (!multiRow)
                                {
                                    DateTime? time = record.GetTime()?.ToDateTimeUtc();
                                    if (time != null)
                                    {
                                        values["Timestamp"] = time.Value;
                                    }

                                    if (record.GetValue() != null)
                                    {
                                        values["OPCUANodeValue"] = record.GetValue();
                                    }

                                    foreach (KeyValuePair<string, object> column in record.Values)
                                    {
                                        if ((column.Value != null) && !column.Key.StartsWith("_"))
                                        {
                                            values[column.Key] = column.Value;
                                        }
                                    }
                                }
                                else
                                {
                                    string field = record.GetField();
                                    if (!string.IsNullOrEmpty(field) && (record.GetValue() != null))
                                    {
                                        values[field] = record.GetValue();
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Debug.WriteLine(ex.Message);

                                // ignore this record and move on
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("RunInfluxQuery: " + ex.Message);
            }

            return values;
        }

        /// <summary>
        /// Builds a Flux query returning the last known value of an OPC UA variable published by the
        /// given station of the given production line within the last hour.
        /// </summary>
        public string BuildLastKnownValueQuery(string stationName, string productionLineName, string valueToQuery)
        {
            return BuildTelemetryQuery(stationName, productionLineName,
                $" |> filter(fn: (r) => r._field == \"{EscapeFlux(valueToQuery)}\")",
                "-1h",
                "now()")
                + " |> last()";
        }

        /// <summary>
        /// Builds a Flux query returning the values of an OPC UA variable published by the given
        /// station of the given production line within +/- the ideal cycle time around the given
        /// point in time.
        /// </summary>
        public string BuildValueAtTimeQuery(string stationName, string productionLineName, string valueToQuery, DateTime timeToQuery, int idealCycleTime)
        {
            DateTime start = timeToQuery.ToUniversalTime().AddSeconds(-idealCycleTime);
            DateTime stop = timeToQuery.ToUniversalTime().AddSeconds(idealCycleTime);

            return BuildTelemetryQuery(stationName, productionLineName,
                $" |> filter(fn: (r) => r._field == \"{EscapeFlux(valueToQuery)}\")",
                start.ToString("o", CultureInfo.InvariantCulture),
                stop.ToString("o", CultureInfo.InvariantCulture))
                + " |> last()";
        }

        /// <summary>
        /// Builds the common part of a telemetry query: it narrows the telemetry measurement to the
        /// dataset writers whose metadata DataSetName ("metaName" tag) contains both the station and
        /// the production line name, which is the InfluxDB equivalent of the ADX join of
        /// opcua_telemetry to opcua_metadata_lkv on Subject.
        /// </summary>
        private string BuildTelemetryQuery(string stationName, string productionLineName, string fieldFilter, string rangeStart, string rangeStop)
        {
            List<string> writers = GetWriters(stationName, productionLineName);
            string writerFilter = string.Empty;
            if (writers.Count > 0)
            {
                string set = string.Join(", ", writers.Select(writer => $"\"{EscapeFlux(writer)}\""));
                writerFilter = $" |> filter(fn: (r) => contains(value: r.datasetWriterId, set: [{set}]))";
            }

            return $"from(bucket: \"{EscapeFlux(Bucket)}\")"
                 + $" |> range(start: {rangeStart}, stop: {rangeStop})"
                 + $" |> filter(fn: (r) => r._measurement == \"{EscapeFlux(Measurement)}\")"
                 + writerFilter
                 + fieldFilter;
        }

        /// <summary>
        /// Returns the datasetWriterIds whose metadata DataSetName contains both the station and the
        /// production line name.
        /// </summary>
        private List<string> GetWriters(string stationName, string productionLineName)
        {
            List<string> writers = new();

            if (_influxClient == null)
            {
                return writers;
            }

            string flux = "import \"strings\"\n"
                + $"from(bucket: \"{EscapeFlux(Bucket)}\")"
                + " |> range(start: -30d)"
                + $" |> filter(fn: (r) => r._measurement == \"{EscapeFlux(MetadataMeasurement)}\")"
                + $" |> filter(fn: (r) => strings.containsStr(v: r.metaName, substr: \"{EscapeFlux(stationName)}\"))"
                + $" |> filter(fn: (r) => strings.containsStr(v: r.metaName, substr: \"{EscapeFlux(productionLineName)}\"))"
                + " |> keep(columns: [\"datasetWriterId\"])"
                + " |> group()"
                + " |> distinct(column: \"datasetWriterId\")";

            try
            {
                List<FluxTable> tables = _influxClient.GetQueryApi().QueryAsync(flux, Org).GetAwaiter().GetResult();
                foreach (FluxTable table in tables)
                {
                    foreach (FluxRecord record in table.Records)
                    {
                        string writer = record.GetValue()?.ToString();
                        if (!string.IsNullOrEmpty(writer) && !writers.Contains(writer))
                        {
                            writers.Add(writer);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("GetWriters: " + ex.Message);
            }

            return writers;
        }

        private static string EscapeFlux(string value)
        {
            return value?.Replace("\\", "\\\\").Replace("\"", "\\\"") ?? string.Empty;
        }
    }
}
