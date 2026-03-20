namespace LsmTree

open System
open System.IO
open System.Text

type WAL(path: string) =
    let stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read)
    let writer = new StreamWriter(stream, Encoding.UTF8)

    do writer.AutoFlush <- true

    member this.Put(seq: int64, key: string, value: string) =
        let k = Convert.ToBase64String(Encoding.UTF8.GetBytes(key))
        let v = Convert.ToBase64String(Encoding.UTF8.GetBytes(value))
        writer.WriteLine(sprintf "PUT %d %s %s" seq k v)

    member this.Delete(seq: int64, key: string) =
        let k = Convert.ToBase64String(Encoding.UTF8.GetBytes(key))
        writer.WriteLine(sprintf "DEL %d %s" seq k)

    member this.Clear() =
        writer.Close()
        stream.Close()
        File.Delete(path)

    member this.Close() =
        writer.Close()
        stream.Close()

module WALRecovery =
    let recover (path: string) =
        if not (File.Exists(path)) then
            Seq.empty
        else
            File.ReadLines(path)
            |> Seq.choose (fun line ->
                let parts = line.Split(' ')

                match parts.[0] with
                | "PUT" when parts.Length = 4 ->
                    let seq = Int64.Parse(parts.[1])
                    let k = Encoding.UTF8.GetString(Convert.FromBase64String(parts.[2]))
                    let v = Encoding.UTF8.GetString(Convert.FromBase64String(parts.[3]))
                    Some(seq, k, Some v)
                | "DEL" when parts.Length = 3 ->
                    let seq = Int64.Parse(parts.[1])
                    let k = Encoding.UTF8.GetString(Convert.FromBase64String(parts.[2]))
                    Some(seq, k, None)
                | _ -> None)
