# Azure Blob Backup

Azure provides blob storage redundancy to guarantee the safety of your blob data in case of an outage at Microsoft. However if you accidentally delete your own blob files either by using a blob explorer tool or by way of a bug in your application, you're out of luck. This library provides an easy way to backup snapshots of your blob storage containers and keep a set number of latest backups to fall back on if necessary.

## Using

1. Grab the [NuGet package](https://www.nuget.org/packages/AzureBlobBackup/) (AzureBlobBackup)
2. Add a reference to FSharp.Core 4.4.0.0
3. Code! (Works great in periodic WebJobs...)

      var blobClient = new BlobClient("YOUR BLOB CONNECTION STRING");
      var oldBackupsToKeep = 3;
      BackupAgent.BackupAsync(blobClient, oldBackupsToKeep).Wait();

## Disclaimer

Use at your own risk. I use this to backup production blob storage accounts, but if you use it and on some off chance it deletes all your blobs and kills the family cat don't come running to me. If you're unsure about anything read the code; it's only about 100 lines of beautiful F#. Or better still, add a unit test or two!