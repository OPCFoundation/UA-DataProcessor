using Newtonsoft.Json.Linq;

namespace UA_DataProcessor.Interfaces
{
    public interface IDataSpaceTransferService
    {
        Task<(string AckStatus, string Message)> StartTransferAsync(string transferId, JObject payload, CancellationToken cancellationToken);
        Task<(string AckStatus, string Message)> SuspendTransferAsync(string transferId, JObject payload, CancellationToken cancellationToken);
        Task<(string AckStatus, string Message)> TerminateTransferAsync(string transferId, JObject payload, CancellationToken cancellationToken);
        Task ExecuteUntypedAsync(JObject payload, CancellationToken cancellationToken);
    }
}

