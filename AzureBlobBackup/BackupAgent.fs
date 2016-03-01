module AzureBlobBackup.BackupAgent

open System
open System.Globalization
open FSharp.Control
open FSharp.Text.RegexProvider

type internal BackupContainerRegex = Regex< @"(?<SourceName>.*)-backup-(?<Timestamp>\d*)$" >
let internal backupContainerRegex = BackupContainerRegex()

let BackupAsync (blobClient : IBlobClient) backupsToKeep =
    if backupsToKeep < 1 then
        invalidArg "You must keep at least one backup." "backupsToKeep"

    let timestampFormat = "yyyyMMddHHmmssFFF"

    let parseTimestamp timestamp = DateTime.ParseExact(timestamp, timestampFormat, CultureInfo.InvariantCulture)
    let formatTimestamp (timestamp : DateTime) = timestamp.ToString(timestampFormat)

    let isBackupContainer (container : ICloudBlobContainer) =
        let regexMatch = backupContainerRegex.Match container.Name
        regexMatch.Success

    let createTargetContainerName originalName =
        sprintf "%s-backup-%s" originalName <| formatTimestamp DateTime.Now

    async {
        let! containersSeq = blobClient.ListAllContainersAsync ()
        let! preBackupContainers = containersSeq |> AsyncSeq.toArrayAsync
        let containersToBackup = preBackupContainers |> Seq.filter (not << isBackupContainer)

        let tooLongContainerNames = containersToBackup |> Seq.filter (fun x -> x.Name.Length > 38) |> Seq.map (fun x -> x.Name)
        if not (tooLongContainerNames |> Seq.isEmpty) then
            invalidOp <| sprintf "The container names %s are too long to backup. Container names must not exceed 38 characters." (String.Join(", ", tooLongContainerNames))

        containersToBackup
            |> Seq.iter (fun sourceContainer ->
                async {
                    let! sourceBlobs = blobClient.ListContainerBlobsAsync sourceContainer
                    let targetContainerName = createTargetContainerName sourceContainer.Name
                    let copyBlobAsync = blobClient.CopyBlobAsync targetContainerName
                    do! sourceBlobs |> AsyncSeq.iterAsync copyBlobAsync
                } |> Async.RunSynchronously
            )

        let preBackupContainersToKeep = backupsToKeep - 1;
        preBackupContainers
        |> Seq.filter isBackupContainer
        |> Seq.groupBy (fun x -> (backupContainerRegex.Match x.Name).SourceName.Value)
        |> Seq.iter (fun (sourceName, backupContainers) ->
            let backupContainersCount = backupContainers |> Seq.length
            if backupContainersCount > preBackupContainersToKeep then
                backupContainers
                |> Seq.sortByDescending (fun container -> container.Name)
                |> Seq.skip preBackupContainersToKeep
                |> Seq.iter (fun x ->
                    async {
                        let foo = ""
                        do! blobClient.DeleteContainerAsync x
                    } |> Async.RunSynchronously
                )
        )
    } |> Async.StartAsTask