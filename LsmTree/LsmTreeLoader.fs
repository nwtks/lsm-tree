namespace LsmTree

module LsmTreeLoader =
    let validateCompactLevelLimits (limits: int[]) =
        if limits.Length = 0 then
            invalidArg "compactLevelLimits" "compactLevelLimits must not be empty"

        if limits |> Array.exists (fun x -> x < 0) then
            invalidArg "compactLevelLimits" "compactLevelLimits must not contain negative values"

        limits

    let parseSstLevel (path: string) =
        let name = System.IO.Path.GetFileName path

        if name.StartsWith "L" then
            let uScore = name.IndexOf '_'

            if uScore > 1 then
                match System.Int32.TryParse(name.Substring(1, uScore - 1)) with
                | true, lvl -> lvl
                | _ ->
                    eprintfn "[WARN] LsmTreeLoader: malformed SSTable name '%s' treated as level 0" name
                    0
            else
                eprintfn "[WARN] LsmTreeLoader: malformed SSTable name '%s' treated as level 0" name
                0
        else
            0

    let compareSSTables (a: SSTable) (b: SSTable) =
        let cmp = compare b.MaxSeq a.MaxSeq
        if cmp <> 0 then cmp else compare a.Path b.Path

    let loadSSTableFiles dataDir (ssTables: SSTable list[]) =
        let mutable maxSeq = 0L

        System.IO.Directory.GetFiles(dataDir, "*.sst")
        |> Array.iter (fun path ->
            let level = parseSstLevel path

            if level < ssTables.Length then
                let sst = new SSTable(path)
                ssTables.[level] <- sst :: ssTables.[level]

                if sst.MaxSeq > maxSeq then
                    maxSeq <- sst.MaxSeq)

        maxSeq

    let sortLevelTables (ssTables: SSTable list[]) =
        for i = 0 to ssTables.Length - 1 do
            ssTables.[i] <- ssTables.[i] |> List.sortWith compareSSTables

    let cleanupTempFiles dataDir =
        System.IO.Directory.GetFiles(dataDir, "*.sst.tmp")
        |> Array.iter System.IO.File.Delete

    let loadSSTables dataDir (ssTables: SSTable list[]) (snapshotManager: LsmTreeSnapshot) =
        cleanupTempFiles dataDir
        let maxSeq = loadSSTableFiles dataDir ssTables
        sortLevelTables ssTables

        if maxSeq > 0L then
            snapshotManager.AdvanceSequence maxSeq

    let collectWalEntries dataDir currentSeq =
        let logs = System.IO.Directory.GetFiles(dataDir, "wal*.log")
        let olds = System.IO.Directory.GetFiles(dataDir, "wal*.old")

        Array.append logs olds
        |> Seq.collect WALRecovery.recover
        |> Seq.filter (fun (seq, _, _) -> seq > currentSeq)
        |> Seq.sortBy (fun (seq, _, _) -> seq)

    let applyEntry (memTable: MemTable) (snapshotManager: LsmTreeSnapshot) (seq, k, vOpt) =
        match vOpt with
        | Some v -> memTable.Put(k, seq, v)
        | None -> memTable.Delete(k, seq)

        snapshotManager.AdvanceSequence seq

    let loadWal dataDir (memTable: MemTable) (snapshotManager: LsmTreeSnapshot) =
        let currentSeq = snapshotManager.CurrentSequence()

        collectWalEntries dataDir currentSeq
        |> Seq.iter (applyEntry memTable snapshotManager)
