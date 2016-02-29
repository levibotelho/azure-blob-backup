namespace AzureBlobBackup.Example
{
    class Program
    {
        static void Main(string[] args)
        {
            var blobClient = new BlobClient("UseDevelopmentStorage=true");
            BackupAgent.BackupAsync(blobClient, 3).Wait();
        }
    }
}
