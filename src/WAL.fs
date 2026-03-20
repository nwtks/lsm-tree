namespace LsmTree

open System
open System.IO
open System.Text

module WALRecovery =
    [<Literal>]
    let PUT = "PUT"

    [<Literal>]
    let DEL = "DEL"

    let utf8ToBase64 (value: string) =
        value |> Encoding.UTF8.GetBytes |> Convert.ToBase64String

    let base64ToUtf8 value =
        value |> Convert.FromBase64String |> Encoding.UTF8.GetString

    let recoverItem (item: string) =
        let parts = item.Split ' '

        match parts.[0] with
        | PUT when parts.Length = 4 ->
            let seq = Int64.Parse parts.[1]
            let k = base64ToUtf8 parts.[2]
            let v = base64ToUtf8 parts.[3]
            Some(seq, k, Some v)
        | DEL when parts.Length = 3 ->
            let seq = Int64.Parse parts.[1]
            let k = base64ToUtf8 parts.[2]
            Some(seq, k, None)
        | _ -> None

    let recover path =
        if not (File.Exists path) then
            Seq.empty
        else
            File.ReadLines path |> Seq.choose recoverItem

type WAL(path: string) =
    let stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read)
    let writer = new StreamWriter(stream, Encoding.UTF8)
    do writer.AutoFlush <- true

    member _.Put(seq: int64, key: string, value: string) =
        let k = WALRecovery.utf8ToBase64 key
        let v = WALRecovery.utf8ToBase64 value
        sprintf "%s %d %s %s" WALRecovery.PUT seq k v |> writer.WriteLine

    member _.Delete(seq: int64, key: string) =
        let k = WALRecovery.utf8ToBase64 key
        sprintf "%s %d %s" WALRecovery.DEL seq k |> writer.WriteLine

    member this.Clear() =
        this.Close()
        File.Delete path

    member _.Close() =
        writer.Close()
        stream.Close()
