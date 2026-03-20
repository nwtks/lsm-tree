namespace LsmTree

open System.IO
open System.Collections.Generic
open System.Threading.Tasks
open System.Threading

type LsmTree(dataDir: string, ?memTableSizeLimit: int) =
    let memTableLimit = defaultArg memTableSizeLimit (1024 * 1024)
    let walPath = Path.Combine(dataDir, "wal.log")
    let memTable = MemTable()
    let mainLock = obj ()

    let maxLevels = 4
    let compactLevelLimits = [| 4; 10; 100; 1000 |]
    let ssTables = Array.init maxLevels (fun _ -> List<SSTable>())
    let ssTablesLock = obj ()

    let mutable globalSeq = 0L

    let parseSstLevel (path: string) =
        let name = Path.GetFileName path

        if name.StartsWith "L" then
            System.Int32.Parse(name.Substring(1, name.IndexOf '_' - 1))
        else
            0

    let startup dataDir =
        if not (Directory.Exists dataDir) then
            Directory.CreateDirectory dataDir |> ignore

        Directory.GetFiles(dataDir, "*.sst")
        |> Array.iter (fun path ->
            let level = parseSstLevel path

            if level < ssTables.Length then
                ssTables.[level].Add(SSTable path))

        ssTables
        |> Array.iter (fun sst ->
            let sorted = sst |> Seq.sortByDescending (fun t -> t.Path)
            sst.Clear()
            sst.AddRange sorted)

        WALRecovery.recover walPath
        |> Seq.iter (function
            | seq, k, Some v ->
                memTable.Put(k, seq, v)
                globalSeq <- max globalSeq seq
            | seq, k, None ->
                memTable.Delete(k, seq)
                globalSeq <- max globalSeq seq)

    do startup dataDir
    let mutable wal = WAL walPath

    let timestamp () =
        System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

    let newGuid () = System.Guid.NewGuid().ToString "N"

    let sstPath level =
        Path.Combine(dataDir, sprintf "L%d_%d_%s.sst" level (timestamp ()) (newGuid ()))

    let rec compact level =
        let tablesToCompact =
            lock ssTablesLock (fun () ->
                if
                    level < ssTables.Length - 1
                    && ssTables.[level].Count > compactLevelLimits.[level]
                then
                    ssTables.[level] |> Seq.toList
                else
                    [])

        if tablesToCompact.Length > 0 then
            // To support MVCC perfectly, we don't drop shadow versions unless we do GC against lowest active snapshot.
            // For now, we write out ALL versions, grouping by key.
            let mergedData = Dictionary<string, List<int64 * string option>>()

            tablesToCompact
            |> List.rev
            |> Seq.collect (fun t -> t.GetAll())
            |> Seq.iter (fun (key, seq, valOpt) ->
                if not (mergedData.ContainsKey key) then
                    mergedData.[key] <- List<int64 * string option>()

                mergedData.[key].Add((seq, valOpt)))

            let finalEntries =
                mergedData
                |> Seq.collect (fun kv -> kv.Value |> Seq.map (fun (s, v) -> kv.Key, s, v))
                |> Seq.sortWith (fun (k1, s1, _) (k2, s2, _) ->
                    let c = System.String.CompareOrdinal(k1, k2)
                    if c <> 0 then c else s2.CompareTo s1)
                |> Seq.toList

            let newSst = SSTableWriter.flush finalEntries (sstPath (level + 1))

            lock ssTablesLock (fun () ->
                ssTables.[level + 1].Insert(0, newSst)

                let remaining =
                    ssTables.[level]
                    |> Seq.filter (fun t -> not (List.contains t tablesToCompact))
                    |> Seq.toList

                ssTables.[level].Clear()
                ssTables.[level].AddRange remaining)

            tablesToCompact
            |> Seq.iter (fun t ->
                try
                    File.Delete t.Path
                with _ ->
                    ())

            compact (level + 1)

    let mutable isCompacting = false

    let triggerCompaction () =
        let shouldStart =
            lock ssTablesLock (fun () ->
                if not isCompacting then
                    isCompacting <- true
                    true
                else
                    false)

        if shouldStart then
            Task.Run(fun () ->
                try
                    compact 0
                finally
                    lock ssTablesLock (fun () -> isCompacting <- false))
            |> ignore

    let flushMemTable () =
        lock ssTablesLock (fun () ->
            if memTable.SizeBytes > 0 then
                let sst = SSTableWriter.flush memTable.Entries (sstPath 0)
                ssTables.[0].Insert(0, sst)

                memTable.Clear()
                wal.Clear()
                wal <- WAL walPath)

        triggerCompaction ()

    member _.Snapshot() = Interlocked.Read(&globalSeq)

    member this.Put(key: string, value: string) =
        let shouldFlush =
            lock mainLock (fun () ->
                let seq = Interlocked.Increment(&globalSeq)
                wal.Put(seq, key, value)
                memTable.Put(key, seq, value)
                memTable.SizeBytes >= memTableLimit)

        if shouldFlush then
            this.Flush()

    member this.Delete(key: string) =
        let shouldFlush =
            lock mainLock (fun () ->
                let seq = Interlocked.Increment(&globalSeq)
                wal.Delete(seq, key)
                memTable.Delete(key, seq)
                memTable.SizeBytes >= memTableLimit)

        if shouldFlush then
            this.Flush()

    member _.Get(key: string, ?snapshot: int64) =
        let snap = defaultArg snapshot (Interlocked.Read(&globalSeq))

        let rec search tbls =
            match tbls with
            | [] -> None
            | t: SSTable :: rest ->
                match t.Get(key, snap) with
                | Some res -> Some res
                | None -> search rest

        let rec searchLevel level =
            if level >= ssTables.Length then
                None
            else
                let tables = lock ssTablesLock (fun () -> ssTables.[level] |> Seq.toList)

                match search tables with
                | Some res -> Some res
                | None -> searchLevel (level + 1)

        let memRes = lock mainLock (fun () -> memTable.Get(key, snap))

        match memRes with
        | Some(Some v) -> Some v
        | Some None -> None
        | None ->
            match searchLevel 0 with
            | Some(Some v) -> Some v
            | _ -> None

    member _.Flush() = lock mainLock flushMemTable

    member _.WaitForCompaction() =
        let rec wait () =
            let active = lock ssTablesLock (fun () -> isCompacting)

            if active then
                Thread.Sleep 50
                wait ()

        wait ()
