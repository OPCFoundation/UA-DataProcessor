namespace UA_DataProcessor.Interfaces
{
    public interface ITransferLifecycleHandler
    {
        Task HandleMessageAsync(IMessageConnection connection, string rawMessage, CancellationToken cancellationToken);
    }
}

