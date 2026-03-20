namespace LsmTree

open System.IO
open System.Collections.Generic
open System.Threading.Tasks
open System.Threading

type LsmTree(dataDir: string, ?memTableSizeLimit: int) =
    do
        if not (Directory.Exists(dataDir)) then
            Directory.CreateDirectory(dataDir) |> ignore

    let memTableLimit = defaultArg memTableSizeLimit (1024 * 1024)
    let walPath = Path.Combine(dataDir, "wal.log")

    let mainLock = obj ()
    let ssTablesLock = obj ()

    let mutable memTable = new MemTable()
    let mutable wal = new WAL(walPath)

    let maxLevels = 4
    let levelLimits = [| 4; 10; 100; 1000 |]

    let ssTables = Array.init maxLevels (fun _ -> new List<SSTable>())
    let mutable isCompacting = false
    let mutable globalSeq = 0L

    do
        let existingTables = Directory.GetFiles(dataDir, "*.sst")

        let parsedTables =
            existingTables
            |> Array.map (fun path ->
                let name = Path.GetFileName(path)

                let level =
                    if name.StartsWith("L") then
                        System.Int32.Parse(name.Substring(1, name.IndexOf('_') - 1))
                    else
                        0

                (level, new SSTable(path)))

        for level, sst in parsedTables do
            if level < maxLevels then
                ssTables.[level].Add(sst)

        for i = 0 to maxLevels - 1 do
            let sorted = ssTables.[i] |> Seq.sortByDescending (fun t -> t.Path) |> Seq.toList
            ssTables.[i].Clear()
            ssTables.[i].AddRange(sorted)

        let recovered = WALRecovery.recover walPath

        for op in recovered do
            match op with
            | seq, k, Some v ->
                memTable.Put(k, seq, v)
                globalSeq <- max globalSeq seq
            | seq, k, None ->
                memTable.Delete(k, seq)
                globalSeq <- max globalSeq seq

    let rec compact level =
        let mutable tablesToCompact = []

        lock ssTablesLock (fun () ->
            if level < maxLevels - 1 && ssTables.[level].Count > levelLimits.[level] then
                tablesToCompact <- ssTables.[level] |> Seq.toList)

        if tablesToCompact.Length > 0 then
            let oldestFirst = tablesToCompact |> List.rev

            // To support MVCC perfectly, we don't drop shadow versions unless we do GC against lowest active snapshot.
            // For now, we write out ALL versions, grouping by key.
            let mergedData = Dictionary<string, List<int64 * string option>>()

            for t in oldestFirst do
                for key, seq, valOpt in t.GetAll() do
                    if not (mergedData.ContainsKey(key)) then
                        mergedData.[key] <- new List<int64 * string option>()

                    mergedData.[key].Add((seq, valOpt))

            // Re-flatten and sort: Key Ascending, Seq Descending
            let finalEntries =
                mergedData
                |> Seq.collect (fun kv -> kv.Value |> Seq.map (fun (s, v) -> (kv.Key, s, v)))
                |> Seq.sortWith (fun (k1, s1, _) (k2, s2, _) ->
                    let c = System.String.CompareOrdinal(k1, k2)
                    if c <> 0 then c else s2.CompareTo(s1))
                |> Seq.toList

            let timestamp = System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

            let newPath =
                Path.Combine(
                    dataDir,
                    sprintf "L%d_%d_%s.sst" (level + 1) timestamp (System.Guid.NewGuid().ToString("N"))
                )

            let newSst = SSTableBuilder.flush finalEntries newPath

            lock ssTablesLock (fun () ->
                ssTables.[level + 1].Insert(0, newSst)

                let remaining =
                    ssTables.[level]
                    |> Seq.filter (fun t -> not (List.contains t tablesToCompact))
                    |> Seq.toList

                ssTables.[level].Clear()
                ssTables.[level].AddRange(remaining))

            for t in tablesToCompact do
                try
                    File.Delete(t.Path)
                with _ ->
                    ()

            compact (level + 1)

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
                let timestamp = System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

                let sstPath =
                    Path.Combine(dataDir, sprintf "L0_%d_%s.sst" timestamp (System.Guid.NewGuid().ToString("N")))

                let sst = SSTableBuilder.flush memTable.Entries sstPath
                ssTables.[0].Insert(0, sst)

                memTable.Clear()
                wal.Clear()
                wal <- new WAL(walPath))

        triggerCompaction ()

    member this.Snapshot() = Interlocked.Read(&globalSeq)

    member this.Put(key: string, value: string) =
        let shouldFlush =
            lock mainLock (fun () ->
                let seq = Interlocked.Increment(&globalSeq)
                wal.Put(seq, key, value)
                memTable.Put(key, seq, value)
                memTable.SizeBytes >= memTableLimit)

        if shouldFlush then
            lock mainLock (fun () -> flushMemTable ())

    member this.Delete(key: string) =
        let shouldFlush =
            lock mainLock (fun () ->
                let seq = Interlocked.Increment(&globalSeq)
                wal.Delete(seq, key)
                memTable.Delete(key, seq)
                memTable.SizeBytes >= memTableLimit)

        if shouldFlush then
            lock mainLock (fun () -> flushMemTable ())

    // Defaults to passing current globalSeq Snapshot
    member this.Get(key: string, ?snapshot: int64) =
        let snap = defaultArg snapshot (Interlocked.Read(&globalSeq))
        let memRes = lock mainLock (fun () -> memTable.Get(key, snap))

        match memRes with
        | Some(Some v) -> Some v
        | Some None -> None
        | None ->
            let rec searchLevel level =
                if level >= maxLevels then
                    None
                else
                    let tables = lock ssTablesLock (fun () -> ssTables.[level] |> Seq.toList)

                    let rec search tbls =
                        match tbls with
                        | [] -> None
                        | (t: SSTable) :: rest ->
                            match t.Get(key, snap) with
                            | Some res -> Some res
                            | None -> search rest

                    match search tables with
                    | Some res -> Some res
                    | None -> searchLevel (level + 1)

            match searchLevel 0 with
            | Some(Some v) -> Some v
            | _ -> None

    member this.Flush() =
        lock mainLock (fun () -> flushMemTable ())

    member this.WaitForCompaction() =
        let rec wait () =
            let active = lock ssTablesLock (fun () -> isCompacting)

            if active then
                Thread.Sleep(50)
                wait ()

        wait ()
