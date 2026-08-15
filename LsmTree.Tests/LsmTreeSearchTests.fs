module LsmTree.Tests.LsmTreeSearchTests

open Xunit
open LsmTree

let writeSst dataDir name entries =
    let path = System.IO.Path.Combine(dataDir, name)
    SSTableWriter.write path entries |> ignore
    path

let keysOfSources (sources: (string * int64 * string option)[] list) =
    sources
    |> List.collect Array.toList
    |> List.map (fun (k, _, _) -> k)
    |> Set.ofList

[<Fact>]
let ``LsmTreeSearch collectMemSources returns memtable and immutable entries`` () =
    let mt = MemTable()
    mt.Put("a", 1L, "va")

    let imm = MemTable()
    imm.Put("b", 2L, "vb")

    let result = LsmTreeSearch.collectMemSources mt (Some imm) "a" "z"
    assertEqual 2 result.Length "both memtable and immutable contribute sources"
    assertEqual (set [ "a"; "b" ]) (keysOfSources result) "keys from both memtables"

[<Fact>]
let ``LsmTreeSearch collectMemSources uses memtable only when no immutable`` () =
    let mt = MemTable()
    mt.Put("a", 1L, "va")
    let result = LsmTreeSearch.collectMemSources mt None "a" "z"
    assertEqual 1 result.Length "single memtable source"
    assertEqual (set [ "a" ]) (keysOfSources result) "key from memtable"

[<Fact>]
let ``LsmTreeSearch collectSstSources skips disposed tables`` () =
    withTestDir "ssearch_collect_sst" (fun dir ->
        let alivePath =
            writeSst dir "L0_alive.sst" [ "a", 1L, Some "va"; "b", 2L, Some "vb" ]

        let disposedPath = writeSst dir "L0_disposed.sst" [ "c", 3L, Some "vc" ]
        use alive = new SSTable(alivePath)

        let disposed = new SSTable(disposedPath)
        (disposed :> System.IDisposable).Dispose()

        let ssTables = [| [ alive; disposed ] |]
        let result = LsmTreeSearch.collectSstSources ssTables "a" "c"
        assertEqual 1 result.Length "only alive source contributes"
        assertEqual 2 result.[0].Length "alive table has two entries")

[<Fact>]
let ``LsmTreeSearch collectSstSourcesFromSnapshot flags disposal and skips rest`` () =
    withTestDir "ssearch_snapshot_disposed" (fun dir ->
        let p1 = writeSst dir "L0_a.sst" [ "a", 1L, Some "va" ]
        let p2 = writeSst dir "L0_b.sst" [ "b", 2L, Some "vb" ]
        let p3 = writeSst dir "L0_c.sst" [ "c", 3L, Some "vc" ]
        use s1 = new SSTable(p1)
        use s3 = new SSTable(p3)

        let s2 = new SSTable(p2)
        (s2 :> System.IDisposable).Dispose()

        let snapshot = [| [ s1; s2; s3 ] |]
        let disposed, sources = LsmTreeSearch.collectSstSourcesFromSnapshot "a" "c" snapshot
        assertEqual true disposed "disposal detected"
        assertEqual 1 sources.Length "only source before the disposed table survives"
        assertEqual (set [ "a" ]) (keysOfSources sources) "key before disposal")

[<Fact>]
let ``LsmTreeSearch snapshotStable detects replaced level list`` () =
    withTestDir "ssearch_snapshot_stable" (fun dir ->
        let p1 = writeSst dir "L0_a.sst" [ "a", 1L, Some "va" ]
        let p2 = writeSst dir "L0_b.sst" [ "b", 2L, Some "vb" ]
        use s1 = new SSTable(p1)
        use s2 = new SSTable(p2)
        let ssTablesLock = obj ()
        let ssTables = [| [ s1 ] |]

        assertEqual
            false
            (LsmTreeSearch.snapshotStable ssTablesLock ssTables [| [ s2 ] |])
            "different level list -> not stable"

        assertEqual
            true
            (LsmTreeSearch.snapshotStable ssTablesLock ssTables (Array.copy ssTables))
            "same references -> stable")

[<Fact>]
let ``LsmTreeSearch tryCollectRangeSources retries then falls back on disposal`` () =
    withTestDir "ssearch_retry_disposed" (fun dir ->
        let p1 = writeSst dir "L0_a.sst" [ "a", 1L, Some "va"; "b", 2L, Some "vb" ]
        let p2 = writeSst dir "L0_disposed.sst" [ "c", 3L, Some "vc" ]
        let p3 = writeSst dir "L0_e.sst" [ "e", 5L, Some "ve" ]
        use s1 = new SSTable(p1)
        use s3 = new SSTable(p3)

        let s2 = new SSTable(p2)
        (s2 :> System.IDisposable).Dispose()

        let ssTables = [| [ s1; s2; s3 ] |]
        let mainLock = new System.Threading.ReaderWriterLockSlim()
        let ssTablesLock = obj ()

        let mt = MemTable()
        mt.Put("d", 4L, "vd")

        let imm = MemTable()
        imm.Put("f", 6L, "vf")

        let result =
            LsmTreeSearch.tryCollectRangeSources mainLock mt (Some imm) ssTablesLock ssTables "a" "z" 1 0

        assertEqual 4 result.Length "sources after retry/fallback"

        let keys =
            result |> Array.collect id |> Array.map (fun (k, _, _) -> k) |> Set.ofArray

        assertEqual (set [ "a"; "b"; "e"; "d"; "f" ]) keys "all surviving keys collected")

[<Fact>]
let ``LsmTreeSearch tryCollectRangeSources handles concurrent level replacement`` () =
    withTestDir "ssearch_conc_replace" (fun dir ->
        let p1 = writeSst dir "L0_a.sst" [ "a", 1L, Some "va" ]
        let p2 = writeSst dir "L0_b.sst" [ "b", 2L, Some "vb" ]
        use s1 = new SSTable(p1)
        use s2 = new SSTable(p2)
        let mainLock = new System.Threading.ReaderWriterLockSlim()
        let ssTablesLock = obj ()
        let ssTables = [| [ s1 ] |]
        let mt = MemTable()

        let stop = ref false

        let mutator =
            System.Threading.Tasks.Task.Run(fun () ->
                while not stop.Value do
                    lock ssTablesLock (fun () -> ssTables.[0] <- [ s2 ])
                    lock ssTablesLock (fun () -> ssTables.[0] <- [ s1 ]))

        try
            for _ = 1 to 200 do
                LsmTreeSearch.tryCollectRangeSources mainLock mt None ssTablesLock ssTables "a" "z" 0 0
                |> ignore

                LsmTreeSearch.tryCollectRangeSources mainLock mt None ssTablesLock ssTables "a" "z" 8 0
                |> ignore
        finally
            stop.Value <- true
            mutator.Wait())
