module AzureBlobBackup.BackupAgent

open System
open System.Globalization
open System.Threading.Tasks
open FSharp.Control
open FSharp.Text.RegexProvider
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob

type BackupContainerRegex = Regex< @"(?<SourceName>.*)__BACKUP__(?<Timestamp>\d*)$" >
let backupContainerRegex = BackupContainerRegex()

type ICloudBlobContainer =
    abstract Name : string
    abstract ListBlobsSegmentedAsync : BlobContinuationToken -> Task<BlobResultSegment>

type AbstractedCloudBlobContainer (cloudBlobContainer : CloudBlobContainer) =
    interface ICloudBlobContainer with
        member this.Name = cloudBlobContainer.Name
        member this.ListBlobsSegmentedAsync continuationToken =
            cloudBlobContainer.ListBlobsSegmentedAsync(continuationToken)

type IBlobClient =
    abstract ListContainerBlobsAsync : ICloudBlobContainer -> Async<AsyncSeq<ICloudBlob>>
    abstract ListAllContainersAsync : unit -> Async<AsyncSeq<ICloudBlobContainer>>
    abstract CopyBlobAsync : string -> ICloudBlob -> Async<unit>

type BlobClient (connectionString) =
    let client = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient()

    let listContainersSegmentedAsync continuationToken =
        async {
            let! result = client.ListContainersSegmentedAsync(continuationToken) |> Async.AwaitTask
            let containers =
                result.Results
                |> Seq.map (fun x -> AbstractedCloudBlobContainer x :> ICloudBlobContainer)
                |> AsyncSeq.ofSeq
            return (result.ContinuationToken, containers)
        }

    let listContainerBlobsSegmentedAsync (container : ICloudBlobContainer) continuationToken =
        async {
            let! result = container.ListBlobsSegmentedAsync(continuationToken) |> Async.AwaitTask
            let blobs =
                result.Results
                |> AsyncSeq.ofSeq
                |> AsyncSeq.mapAsync (fun x ->
                    async {
                        return! client.GetBlobReferenceFromServerAsync(x.Uri) |> Async.AwaitTask
                    })
            return (result.ContinuationToken, blobs)
        }

    let rec listAllStorageItemsAsync getItemsAsync continuationToken existingItems =
        async {
            let! newContinuationToken, newItems = getItemsAsync continuationToken
            let allItems = AsyncSeq.append newItems existingItems
            match newContinuationToken with
            | null -> return allItems
            | x -> return! listAllStorageItemsAsync getItemsAsync x allItems
        }

    let rec listAllContainersAsync = listAllStorageItemsAsync listContainersSegmentedAsync
    let rec listContainerBlobsAsync container = listAllStorageItemsAsync <| listContainerBlobsSegmentedAsync container

    interface IBlobClient with
        member this.ListContainerBlobsAsync container = listContainerBlobsAsync container null AsyncSeq.empty
        member this.ListAllContainersAsync () = listAllContainersAsync null AsyncSeq.empty
        member this.CopyBlobAsync targetContainerName (sourceBlob : ICloudBlob) =
            let targetContainer = client.GetContainerReference(targetContainerName)
            async {
                let targetBlob = targetContainer.GetBlobReference(sourceBlob.Name)
                do! targetBlob.StartCopyAsync(sourceBlob.Uri) |> Async.AwaitTask |> Async.Ignore
            }

let BackupAsync (storageClient : IBlobClient) backupsToKeep =
    let timestampFormat = "yyyyMMddHHmmss"

    let parseTimestamp timestamp = DateTime.ParseExact(timestamp, timestampFormat, CultureInfo.InvariantCulture)
    let formatTimestamp (timestamp : DateTime) = timestamp.ToString(timestampFormat)

    let isBackupContainer (container : ICloudBlobContainer) =
        let regexMatch = backupContainerRegex.Match container.Name
        regexMatch.Success

    let createTargetContainerName originalName =
        sprintf "%s__BACKUP__%s" originalName <| formatTimestamp DateTime.Now

    async {
        let! containers = storageClient.ListAllContainersAsync ()
        do! containers
            |> AsyncSeq.filter (not << isBackupContainer)
            |> AsyncSeq.iterAsync (fun sourceContainer ->
                async {
                    let! sourceBlobs = storageClient.ListContainerBlobsAsync sourceContainer
                    let targetContainerName = createTargetContainerName sourceContainer.Name
                    let copyBlobAsync = storageClient.CopyBlobAsync targetContainerName
                    do! sourceBlobs |> AsyncSeq.iterAsync copyBlobAsync
                }
            )
    } |> Async.StartAsTask