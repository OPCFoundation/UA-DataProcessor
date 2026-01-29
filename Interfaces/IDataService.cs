namespace UA_DataProcessor.Interfaces
{
    public interface IDataService : IDisposable
    {
        public void Connect();

        public Dictionary<string, object> RunQuery(string query);
    }
}
