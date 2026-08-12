namespace LsmTree

module internal WALRecovery =
    [<Literal>]
    let PUT = "PUT"

    [<Literal>]
    let DEL = "DEL"

    [<Literal>]
    let BEGIN = "BEGIN"

    [<Literal>]
    let COMMIT = "COMMIT"

    [<Literal>]
    let ABORT = "ABORT"

    type RecoveryEntry =
        | Op of string * string option
        | Begin
        | Commit

    let log msg = eprintfn msg

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
        | ABORT -> None
        | _ -> None

    let parseEntry (item: string) =
        let parts = item.Split ' '

        match tryParseSeq parts with
        | Some seq ->
            try
                parseCommand seq parts
            with :? System.FormatException ->
                log $"[WARN] WAL recovery: skipping malformed line: {item}"
                None
        | None ->
            log $"[WARN] WAL recovery: skipping malformed line: {item}"
            None

    let collectSequenceSets path =
        System.IO.File.ReadLines path
        |> Seq.fold
            (fun (committed, begun) line ->
                match parseEntry line with
                | Some(seq, Begin) -> committed, Set.add seq begun
                | Some(seq, Commit) -> Set.add seq committed, begun
                | _ -> committed, begun)
            (Set.empty, Set.empty)

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

type internal WAL(path: string) =
    let stream =
        new System.IO.FileStream(path, System.IO.FileMode.Append, System.IO.FileAccess.Write, System.IO.FileShare.Read)

    let writer = new System.IO.StreamWriter(stream, System.Text.Encoding.UTF8)
    let walLock = obj ()
    let mutable disposed = false

    let write sync (log: string) =
        lock walLock (fun () ->
            writer.WriteLine log

            if sync then
                writer.Flush()
                stream.Flush true)

    member _.Put(seq: int64, key: string, value: string) =
        let k = WALRecovery.utf8ToBase64 key
        let v = WALRecovery.utf8ToBase64 value
        $"{WALRecovery.PUT} {seq} {k} {v}" |> write false

    member _.PutSingle(seq: int64, key: string, value: string, ?sync: bool) =
        let sync = defaultArg sync true
        let k = WALRecovery.utf8ToBase64 key
        let v = WALRecovery.utf8ToBase64 value
        $"{WALRecovery.PUT} {seq} {k} {v}" |> write sync

    member _.Delete(seq: int64, key: string) =
        let k = WALRecovery.utf8ToBase64 key
        $"{WALRecovery.DEL} {seq} {k}" |> write false

    member _.DeleteSingle(seq: int64, key: string, ?sync: bool) =
        let sync = defaultArg sync true
        let k = WALRecovery.utf8ToBase64 key
        $"{WALRecovery.DEL} {seq} {k}" |> write sync

    member _.Begin(seq: int64) =
        $"{WALRecovery.BEGIN} {seq}" |> write false

    member _.Commit(seq: int64, ?sync: bool) =
        let sync = defaultArg sync true
        $"{WALRecovery.COMMIT} {seq}" |> write sync

    member _.Abort(seq: int64, ?sync: bool) =
        let sync = defaultArg sync true
        $"{WALRecovery.ABORT} {seq}" |> write sync

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
                        WALRecovery.log "[WARN] WAL: I/O error during final flush"

                    try
                        (writer :> System.IDisposable).Dispose()
                    with _ ->
                        ()

                    try
                        (stream :> System.IDisposable).Dispose()
                    with _ ->
                        ())
