namespace LsmTree

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
        value |> System.Text.Encoding.UTF8.GetBytes |> System.Convert.ToBase64String

    let base64ToUtf8 value =
        value |> System.Convert.FromBase64String |> System.Text.Encoding.UTF8.GetString

    let tryParseSeq (parts: string[]) =
        if parts.Length < 2 then
            None
        else
            match System.Int64.TryParse parts.[1] with
            | true, seq -> Some seq
            | _ -> None

    let parsePut seq (parts: string[]) =
        if parts.Length = 4 then
            let k = base64ToUtf8 parts.[2]
            let v = base64ToUtf8 parts.[3]
            Some(seq, Op(k, Some v))
        else
            None

    let parseDel seq (parts: string[]) =
        if parts.Length = 3 then
            let k = base64ToUtf8 parts.[2]
            Some(seq, Op(k, None))
        else
            None

    let parseBeginCommit seq (parts: string[]) entry =
        if parts.Length = 2 then Some(seq, entry) else None

    let parseCommand seq (parts: string[]) =
        match parts.[0] with
        | PUT -> parsePut seq parts
        | DEL -> parseDel seq parts
        | BEGIN -> parseBeginCommit seq parts Begin
        | COMMIT -> parseBeginCommit seq parts Commit
        | _ -> None

    let parseEntry (item: string) =
        let parts = item.Split ' '

        match tryParseSeq parts with
        | Some seq ->
            try
                parseCommand seq parts
            with :? System.FormatException ->
                eprintfn $"[WARN] WAL recovery: skipping malformed line: {item}"
                None
        | None -> None

    let collectSequenceSets path =
        ((Set.empty, Set.empty), System.IO.File.ReadLines path)
        ||> Seq.fold (fun (committed, begun) line ->
            match parseEntry line with
            | Some(seq, Begin) -> committed, Set.add seq begun
            | Some(seq, Commit) -> Set.add seq committed, begun
            | _ -> committed, begun)

    let recoverOps path committedSeqs begunSeqs =
        System.IO.File.ReadLines path
        |> Seq.choose (fun line ->
            match parseEntry line with
            | Some(seq, Op(k, v)) when Set.contains seq committedSeqs || not (Set.contains seq begunSeqs) ->
                Some(seq, k, v)
            | _ -> None)

    let recover path =
        if System.IO.File.Exists path then
            let committedSeqs, begunSeqs = collectSequenceSets path
            recoverOps path committedSeqs begunSeqs
        else
            Seq.empty

type WAL(path: string) =
    let stream =
        new System.IO.FileStream(path, System.IO.FileMode.Append, System.IO.FileAccess.Write, System.IO.FileShare.Read)

    let writer = new System.IO.StreamWriter(stream, System.Text.Encoding.UTF8)
    let walLock = obj ()
    let mutable disposed = false

    let writeSync sync (log: string) =
        lock walLock (fun () ->
            writer.WriteLine log

            if sync then
                writer.Flush()
                stream.Flush true)

    member _.Put(seq: int64, key: string, value: string) =
        let k = WALRecovery.utf8ToBase64 key
        let v = WALRecovery.utf8ToBase64 value
        let log = $"{WALRecovery.PUT} {seq} {k} {v}"
        lock walLock (fun () -> writer.WriteLine log)

    member _.PutSingle(seq: int64, key: string, value: string, ?sync: bool) =
        let sync = defaultArg sync true
        let k = WALRecovery.utf8ToBase64 key
        let v = WALRecovery.utf8ToBase64 value
        $"{WALRecovery.PUT} {seq} {k} {v}" |> writeSync sync

    member _.Delete(seq: int64, key: string) =
        let k = WALRecovery.utf8ToBase64 key
        let log = $"{WALRecovery.DEL} {seq} {k}"
        lock walLock (fun () -> writer.WriteLine log)

    member _.DeleteSingle(seq: int64, key: string, ?sync: bool) =
        let sync = defaultArg sync true
        let k = WALRecovery.utf8ToBase64 key
        $"{WALRecovery.DEL} {seq} {k}" |> writeSync sync

    member _.Begin(seq: int64) =
        let log = $"{WALRecovery.BEGIN} {seq}"
        lock walLock (fun () -> writer.WriteLine log)

    member _.Commit(seq: int64, ?sync: bool) =
        let sync = defaultArg sync true
        $"{WALRecovery.COMMIT} {seq}" |> writeSync sync

    member this.Close() = (this :> System.IDisposable).Dispose()

    interface System.IDisposable with
        member _.Dispose() =
            if not disposed then
                disposed <- true

                lock walLock (fun () ->
                    try
                        writer.Flush()
                        stream.Flush true
                    with _ ->
                        eprintfn "[WARN] WAL: I/O error during final flush"

                    try
                        (writer :> System.IDisposable).Dispose()
                    with _ ->
                        ()

                    try
                        (stream :> System.IDisposable).Dispose()
                    with _ ->
                        ())
