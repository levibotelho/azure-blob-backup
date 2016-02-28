module AzureBlobBackup.Tests.BackupAgentTests

open System
open System.Threading.Tasks
open AzureBlobBackup
open BackupAgent
open FSharp.Control
open FsUnit
open Microsoft.WindowsAzure.Storage.Blob
open Moq
open NUnit.Framework

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
        member this.DeleteAsync () = async { return () }

type ClientStub () =
    let mutable containers =
        seq {
            yield ("1", container1Blobs |> Seq.toList)
            yield ("2", container2Blobs |> Seq.toList)
        } |> Map.ofSeq

    member this.Containers with get () = containers

    interface IBlobClient with
        member this.ListAllContainersAsync () =
            async {
                return
                    containers
                    |> Seq.map (fun kvp -> DefaultBlobContainer(kvp.Key, null) :> ICloudBlobContainer)
                    |> AsyncSeq.ofSeq
            }

        member this.ListContainerBlobsAsync container =
            async { return containers.[container.Name] |> AsyncSeq.ofSeq }

        member this.CopyBlobAsync targetContainerName sourceBlob =
            async {
                let newBlobs =
                    match containers |> Map.tryFind targetContainerName with
                    | Some blobs -> sourceBlob :: blobs
                    | None -> List.singleton sourceBlob
                containers <- containers.Add(targetContainerName, newBlobs)
                return ()
            }

        member this.DeleteContainerAsync (container : ICloudBlobContainer) =
            containers <- containers.Remove container.Name
            async { return () }

// We delay this by 20 milliseconds to ensure that every blob gets a unique timestamp. DateTime.Now increments
// once every 16 milliseconds or so and we get bugs in these tests if we make two backups in immediate succession
// as the container names will be the same. As we can assume that nobody is going to be backing up their blobs
// more than once per 20ms this delay has no production impact.
let Backup blobClient backupsToKeep =
    Task.Delay(20).Wait()
    let task = BackupAsync blobClient backupsToKeep
    task.Wait()

let getContainerNames (clientStub : ClientStub) =
    clientStub.Containers |> Seq.map (fun x -> x.Key)

let getBackupContainerNames (clientStub : ClientStub) =
    getContainerNames clientStub
    |> Seq.filter (fun x -> x.Contains("__BACKUP__"))

[<Test>]
let ``It works`` () =
    Backup <| ClientStub () <| 1

[<Test>]
let ``It copies all blobs`` () =
    let clientStub = ClientStub ()
    Backup clientStub 1
    let backupContainerNames = getBackupContainerNames clientStub
    let copiedBlobs =
        clientStub.Containers
        |> Seq.filter (fun x -> backupContainerNames |> Seq.contains x.Key)
        |> Seq.collect (fun kvp -> kvp.Value)
    CollectionAssert.AreEquivalent(allBlobs, copiedBlobs)

[<Test>]
let ``It creates one backup container per source container`` () =
    let clientStub = ClientStub ()
    Backup clientStub 1
    let names = getBackupContainerNames clientStub
    Seq.length names |> should equal 2

[<Test>]
let ``It creates backup containers with names consisting of the source name`` () =
    let clientStub = ClientStub ()
    let originalContainerNames = getContainerNames clientStub

    Backup clientStub 1

    let backupContainerNames = getBackupContainerNames clientStub
    let allBackupContainersStartWithARealContainerName =
        backupContainerNames |> Seq.forall (fun backupName -> originalContainerNames |> Seq.exists (fun originalName -> backupName.StartsWith(originalName)))
    allBackupContainersStartWithARealContainerName |> should equal true

[<Test>]
let ``It creates backup containers with names containing the word __BACKUP__`` () =
    let clientStub = ClientStub ()
    Backup clientStub 1
    let names = getBackupContainerNames clientStub
    let allNamesContainBackupText = names |> Seq.forall (fun x -> x.Contains("__BACKUP__"))
    allNamesContainBackupText |> should equal true

[<Test>]
let ``It creates backup containers with names ending in a timestamp`` () =
    let clientStub = ClientStub ()
    Backup clientStub 1
    let names = getBackupContainerNames clientStub
    let allNamesEndInATimestamp = names |> Seq.map (fun x -> backupContainerRegex.Match x) |> Seq.forall (fun x -> x.Timestamp.Success)
    allNamesEndInATimestamp |> should equal true

[<Test>]
let ``It creates backup containers with increasing timestamps`` () =
    let clientStub = ClientStub ()

    let getTimestamps () =
        getBackupContainerNames clientStub
        |> Seq.map (fun x -> backupContainerRegex.Match x)
        |> Seq.map (fun x -> x.Timestamp.Value)

    Backup clientStub 1
    let firstTimestamps = getTimestamps ()
    Backup clientStub 1

    let secondTimestamps =
        getTimestamps ()
        |> Seq.filter (fun x -> not (firstTimestamps |> Seq.contains x))

    Seq.max firstTimestamps |> should be (lessThan <| Seq.min secondTimestamps)

[<Test>]
let ``It keeps the latest old backups that do not exceed the backups count`` () =
    let clientStub = ClientStub ()
    let getBackupContainerNames () = getBackupContainerNames clientStub
    Backup clientStub 2
    Backup clientStub 2
    Backup clientStub 2
    let backupNames = getBackupContainerNames ()
    backupNames |> Seq.distinct |> Seq.length |> should equal 4

[<Test>]
let ``It removes old backups that exceed the old backups count`` () =
    let clientStub = ClientStub ()
    let getBackupContainerNames () = getBackupContainerNames clientStub
    Backup clientStub 2
    let firstBackupNames = getBackupContainerNames ()
    Backup clientStub 2
    let secondBackupNames = getBackupContainerNames ()
    Backup clientStub 2
    let thirdBackupNames = getBackupContainerNames ()
    let expectedOldBackupsHaveBeenErased = firstBackupNames |> Seq.forall (fun x -> not (thirdBackupNames |> Seq.contains x))
    expectedOldBackupsHaveBeenErased |> should equal true