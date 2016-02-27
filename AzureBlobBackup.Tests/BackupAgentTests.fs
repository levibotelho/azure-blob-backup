module AzureBlobBackup.Tests.BackupAgentTests

open System
open System.Threading.Tasks
open AzureBlobBackup.BackupAgent
open FSharp.Control
open FsUnit
open Microsoft.WindowsAzure.Storage.Blob
open Moq
open NUnit.Framework

//type DefaultBlobClient () =
//    interface IBlobClient with
//        member this.ListContainerBlobsAsync container = async { return AsyncSeq.empty }
//        member this.ListAllContainersAsync () = async { return AsyncSeq.empty }
//        member this.CopyBlobAsync targetContainerName sourceBlob = async { return () }

let createBlobMock (name : string) =
    let mock = Mock<ICloudBlob>()
    mock.Setup(fun x -> x.Name).Returns(name) |> ignore
    mock.Object

let aBlob = createBlobMock "A"
let bBlob = createBlobMock "B"
let cBlob = createBlobMock "C"
let dBlob = createBlobMock "D"
let eBlob = createBlobMock "E"

let container1Blobs = seq { yield aBlob; yield bBlob; yield cBlob }
let container2Blobs = seq { yield dBlob; yield eBlob }
let allBlobs = seq { yield! container1Blobs; yield! container2Blobs }

type DefaultBlobContainer (name, blobs) =
    interface ICloudBlobContainer with
        member this.Name = name
        member this.ListBlobsSegmentedAsync continuationToken = Task.FromResult(blobs)

let blobContainers = seq {
    yield DefaultBlobContainer("1", null) :> ICloudBlobContainer
    yield DefaultBlobContainer("2", null) :> ICloudBlobContainer
}

type ClientStub () =
    let mutable targetBlobContainers = Map.empty<string, ICloudBlob list>

    member this.TargetBlobContainers with get () = targetBlobContainers

    interface IBlobClient with
        member this.ListAllContainersAsync () =
            async {
                return (AsyncSeq.ofSeq (seq { yield! blobContainers }))
            }

        member this.ListContainerBlobsAsync container =
            async {
                return
                    match container.Name with
                    | "1" -> container1Blobs |> AsyncSeq.ofSeq
                    | "2" -> container2Blobs |> AsyncSeq.ofSeq
                    | _ -> raise <| NotImplementedException()
            }

        member this.CopyBlobAsync targetContainerName sourceBlob =
            async {
                let newBlobs =
                    match targetBlobContainers |> Map.tryFind targetContainerName with
                    | Some blobs -> sourceBlob :: blobs
                    | None -> List.singleton sourceBlob
                targetBlobContainers <- targetBlobContainers.Add(targetContainerName, newBlobs)
                return ()
            }

let Backup blobClient backupsToKeep =
    let task = BackupAsync blobClient backupsToKeep
    task.Wait()

[<Test>]
let ``It works`` () =
    Backup <| ClientStub () <| 1

[<Test>]
let ``It copies all blobs`` () =
    let clientStub = ClientStub ()
    Backup clientStub 1
    let copiedBlobs = clientStub.TargetBlobContainers |> Seq.collect (fun kvp -> kvp.Value)
    CollectionAssert.AreEquivalent(allBlobs, copiedBlobs)

let getBackupContainerNames (targetBlobContainers : Map<string, ICloudBlob list>) =
    targetBlobContainers |> Seq.map (fun kvp -> kvp.Key)

[<Test>]
let ``It creates one backup container per source container`` () =
    let clientStub = ClientStub ()
    Backup clientStub 1
    let names = getBackupContainerNames clientStub.TargetBlobContainers
    Seq.length names |> should equal <| Seq.length blobContainers

[<Test>]
let ``It creates backup containers with names consisting of the source name`` () =
    let containerNames = blobContainers |> Seq.map (fun x -> x.Name)

    let clientStub = ClientStub ()
    Backup clientStub 1
    let matchedNames =
        getBackupContainerNames clientStub.TargetBlobContainers
        |> Seq.map (fun x -> backupContainerRegex.Match x)

    let backedUpContainerSourceNames = matchedNames |> Seq.map (fun x -> x.SourceName.Value)
    CollectionAssert.AreEquivalent(containerNames, backedUpContainerSourceNames)

[<Test>]
let ``It creates backup containers with names containing the word __BACKUP__`` () =
    let clientStub = ClientStub ()
    Backup clientStub 1
    let names = getBackupContainerNames clientStub.TargetBlobContainers
    let allNamesContainBackupText = names |> Seq.forall (fun x -> x.Contains("__BACKUP__"))
    allNamesContainBackupText |> should equal true

[<Test>]
let ``It creates backup containers with names ending in a timestamp`` () =
    let clientStub = ClientStub ()
    Backup clientStub 1
    let names = getBackupContainerNames clientStub.TargetBlobContainers
    let allNamesEndInATimestamp = names |> Seq.map (fun x -> backupContainerRegex.Match x) |> Seq.forall (fun x -> x.Timestamp.Success)
    allNamesEndInATimestamp |> should equal true

[<Test>]
let ``It creates backup containers with increasing timestamps`` () =
    let clientStub = ClientStub ()

    let getTimestamps () =
        getBackupContainerNames clientStub.TargetBlobContainers
        |> Seq.map (fun x -> backupContainerRegex.Match x)
        |> Seq.map (fun x -> x.Timestamp.Value)

    Backup clientStub 1
    let firstTimestamps = getTimestamps ()
    Task.Delay(1000).Wait()
    Backup clientStub 1

    let secondTimestamps =
        getTimestamps ()
        |> Seq.filter (fun x -> not (firstTimestamps |> Seq.contains x))

    Seq.max firstTimestamps |> should be (lessThan <| Seq.min secondTimestamps)