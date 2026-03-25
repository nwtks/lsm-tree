namespace LsmTree

open System
open System.Collections.Generic
open System.IO
open System.Text

module WALRecovery =
    [<Literal>]
    let PUT = "PUT"

    [<Literal>]
    let DEL = "DEL"

    [<Literal>]
    let BEGIN = "BEGIN"

    [<Literal>]
    let COMMIT = "COMMIT"

    type RecoveryEntry =
        | Op of string * string option
        | Begin
        | Commit

    let utf8ToBase64 (value: string) =
        value |> Encoding.UTF8.GetBytes |> Convert.ToBase64String

    let base64ToUtf8 value =
        value |> Convert.FromBase64String |> Encoding.UTF8.GetString

    let parseEntry (item: string) =
        let parts = item.Split ' '

        if parts.Length < 2 then
            None
        else
            match Int64.TryParse parts.[1] with
            | true, seq ->
                match parts.[0] with
                | PUT when parts.Length = 4 ->
                    let k = base64ToUtf8 parts.[2]
                    let v = base64ToUtf8 parts.[3]
                    Some(seq, Op(k, Some v))
                | DEL when parts.Length = 3 ->
                    let k = base64ToUtf8 parts.[2]
                    Some(seq, Op(k, None))
                | BEGIN when parts.Length = 2 -> Some(seq, Begin)
                | COMMIT when parts.Length = 2 -> Some(seq, Commit)
                | _ -> None
            | _ -> None

    let collectEntries
        (buffered: Dictionary<int64, (string * string option) list>)
        (seq: int64)
        (entry: RecoveryEntry)
        =
        match entry with
        | Begin ->
            buffered.[seq] <- []
            Seq.empty
        | Op(k, v) ->
            if buffered.ContainsKey seq then
                buffered.[seq] <- (k, v) :: buffered.[seq]
                Seq.empty
            else
                Seq.singleton (seq, k, v)
        | Commit ->
            if buffered.ContainsKey seq then
                let ops = buffered.[seq]
                buffered.Remove seq |> ignore
                ops |> List.rev |> Seq.map (fun (k, v) -> seq, k, v)
            else
                Seq.empty

    let recover path =
        if not (File.Exists path) then
            Seq.empty
        else
            let buffered = Dictionary<int64, (string * string option) list>()

            File.ReadLines path
            |> Seq.choose parseEntry
            |> Seq.collect (fun (seq, entry) -> collectEntries buffered seq entry)

type WAL(path: string) =
    let stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read)
    let writer = new StreamWriter(stream, Encoding.UTF8)
    let walLock = obj ()
    let mutable disposed = false
    do writer.AutoFlush <- true

    member _.Put(seq: int64, key: string, value: string) =
        let k = WALRecovery.utf8ToBase64 key
        let v = WALRecovery.utf8ToBase64 value
        let log = sprintf "%s %d %s %s" WALRecovery.PUT seq k v
        lock walLock (fun () -> writer.WriteLine log)

    member _.Delete(seq: int64, key: string) =
        let k = WALRecovery.utf8ToBase64 key
        let log = sprintf "%s %d %s" WALRecovery.DEL seq k
        lock walLock (fun () -> writer.WriteLine log)

    member _.Begin(seq: int64) =
        let log = sprintf "%s %d" WALRecovery.BEGIN seq
        lock walLock (fun () -> writer.WriteLine log)

    member _.Commit(seq: int64, ?sync: bool) =
        let sync = defaultArg sync true
        let log = sprintf "%s %d" WALRecovery.COMMIT seq

        lock walLock (fun () ->
            writer.WriteLine log

            if sync then
                stream.Flush true)

    member this.Close() = (this :> IDisposable).Dispose()

    interface IDisposable with
        member _.Dispose() =
            if not disposed then
                writer.Flush()
                stream.Flush true
                writer.Dispose()
                stream.Dispose()
                disposed <- true
