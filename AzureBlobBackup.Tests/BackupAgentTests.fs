module AzureBlobBackup.Tests.BackupAgentTests

open AzureBlobBackup.BackupAgent
open FSharp.Control
open FsUnit
open NUnit.Framework

type DefaultBlobClient () =
    interface IBlobClient with
        member this.ListContainerBlobsAsync container = async { return AsyncSeq.empty }
        member this.ListAllContainersAsync () = async { return AsyncSeq.empty }
        member this.CopyBlobAsync targetContainerName sourceBlob = async { return () }

let Backup blobClient backupsToKeep =
    let task = BackupAsync blobClient backupsToKeep
    task.Wait()

[<Test>]
let ``It works`` () =
    Backup <| DefaultBlobClient () <| 1