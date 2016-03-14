namespace AzureBlobBackup.Example
{
    class Program
    {
        static void Main(string[] args)
        {
            BackupAgent.BackupAsync("UseDevelopmentStorage=true", 3).Wait();
        }
    }
}
