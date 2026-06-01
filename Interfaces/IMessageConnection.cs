namespace UA_DataProcessor.Interfaces
{
    public interface IMessageConnection
    {
        string ClientId { get; }

        Task RunAsync(Func<IMessageConnection, string, CancellationToken, Task> onMessageAsync, CancellationToken cancellationToken);

        Task SendTextAsync(string text, CancellationToken cancellationToken);

        void Close();
    }
}

