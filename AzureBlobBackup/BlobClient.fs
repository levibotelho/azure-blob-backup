namespace AzureBlobBackup

open System.Threading.Tasks
open FSharp.Control
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob

type ICloudBlobContainer =
    abstract Name : string
    abstract DeleteAsync : unit -> Async<unit>
    abstract ListBlobsSegmentedAsync : BlobContinuationToken -> Task<BlobResultSegment>

type private AbstractedCloudBlobContainer (cloudBlobContainer : CloudBlobContainer) =
    interface ICloudBlobContainer with
        member this.Name = cloudBlobContainer.Name
        member this.DeleteAsync () = async { do! cloudBlobContainer.DeleteAsync() |> Async.AwaitTask }
        member this.ListBlobsSegmentedAsync continuationToken =
            cloudBlobContainer.ListBlobsSegmentedAsync(continuationToken)

type IBlobClient =
    abstract ListContainerBlobsAsync : ICloudBlobContainer -> Async<AsyncSeq<ICloudBlob>>
    abstract ListAllContainersAsync : unit -> Async<AsyncSeq<ICloudBlobContainer>>
    abstract CopyBlobAsync : string -> ICloudBlob -> Async<unit>
    abstract DeleteContainerAsync : ICloudBlobContainer -> Async<unit>

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
        member this.DeleteContainerAsync container =
            async { do! container.DeleteAsync() }

