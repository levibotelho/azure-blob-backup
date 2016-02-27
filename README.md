# Azure Blob Backup

## Why?

Azure provides blob storage redundancy to guarantee the safety of your blob data in case of an outage at Microsoft. However if you accidentally delete your own blob files either by using a blob explorer tool or by way of a bug in your application, you're out of luck. This library provides an easy way to backup blob storage containers to a separate blob storage account.

## Contributing

1. Run `.paket\paket.bootstrapper.exe` to get the latest version of Paket.
2. Run `.paket\paket install` to install all of the project's dependencies.
3. Start developing!