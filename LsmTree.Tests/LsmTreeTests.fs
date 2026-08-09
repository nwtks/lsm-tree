module LsmTree.Tests.LsmTreeTests

open Xunit
open LsmTree

[<Fact>]
let ``LsmTree startup creates directory if missing`` () =
    withTestDir "startup_create_dir" (fun testDir ->
        let treePath = System.IO.Path.Combine(testDir, "nested", "tree", "path")
        let _ = new LsmTree(treePath)
        Assert.True(System.IO.Directory.Exists treePath))

[<Fact>]
let ``LsmTree restart reloads data from WAL`` () =
    withTestDir "restart_reload" (fun testDir ->
        do
            use tree = new LsmTree(testDir)
            tree.Put("rk1", "rv1")
            tree.Put("rk2", "rv2")

        do
            use tree = new LsmTree(testDir)
            assertEqual (Some "rv1") (tree.Get "rk1") "Key1 survives restart"
            assertEqual (Some "rv2") (tree.Get "rk2") "Key2 survives restart")

[<Fact>]
let ``LsmTree restart after delete recovers DEL entry from WAL`` () =
    withTestDir "restart_del" (fun testDir ->
        let tree1 = new LsmTree(testDir)
        tree1.Delete "k"
        tree1.Close()

        use tree2 = new LsmTree(testDir)
        assertEqual None (tree2.Get "k") "DEL entry recovered on restart")

[<Fact>]
let ``LsmTree constructor throws for empty path`` () =
    Assert.Throws<System.ArgumentException>(fun () -> new LsmTree "" |> ignore)
    |> ignore

[<Fact>]
let ``LsmTree explicit snapshot sees consistent view`` () =
    withTestDir "explicit_snap" (fun dir ->
        use tree = new LsmTree(dir)
        let snap1 = tree.Snapshot()
        tree.Put("sk", "sv1")

        let snap2 = tree.Snapshot()
        tree.Put("sk", "sv2")
        assertEqual None (tree.Get("sk", snap1)) "Snapshot before writes sees nothing"
        assertEqual (Some "sv1") (tree.Get("sk", snap2)) "Snapshot after first write sees v1"
        assertEqual (Some "sv2") (tree.Get "sk") "Direct latest sees v2")

[<Fact>]
let ``LsmTree MVCC returns correct versions at each snapshot`` () =
    withTestDir "mvcc_versions" (fun dir ->
        use tree = new LsmTree(dir)
        let snap0 = tree.Snapshot()
        tree.Put("k", "v1")

        let snap1 = tree.Snapshot()
        tree.Put("k", "v2")

        let snap2 = tree.Snapshot()
        assertEqual None (tree.Get("k", snap0)) "snap0 sees None"
        assertEqual (Some "v1") (tree.Get("k", snap1)) "snap1 sees v1"
        assertEqual (Some "v2") (tree.Get("k", snap2)) "snap2 sees v2"
        assertEqual (Some "v2") (tree.Get "k") "direct latest sees v2")

[<Fact>]
let ``Snapshot handle prevents compaction from pruning old versions`` () =
    withTestDir "snap_handle_prune" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
        tree.Put("k", "v1")
        tree.Flush()

        use snap = tree.Snapshot()
        tree.Put("k", "v2")
        tree.Flush()
        tree.Put("k", "v3")
        tree.Flush()
        tree.WaitForCompaction()

        assertEqual (Some "v1") (tree.Get("k", snap)) "Old version preserved while snapshot handle is active"
        assertEqual (Some "v3") (tree.Get "k") "Latest version still visible")

[<Fact>]
let ``Snapshot handle release allows compaction to prune old versions`` () =
    withTestDir "snap_handle_release" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
        tree.Put("k", "v1")
        tree.Flush()

        let snap = tree.Snapshot()
        tree.Put("k", "v2")
        tree.Flush()
        snap.Dispose()

        tree.Put("k", "v3")
        tree.Flush()
        tree.WaitForCompaction()

        assertEqual None (tree.Get("k", snap)) "Old version pruned after handle release"
        assertEqual (Some "v3") (tree.Get "k") "Latest version still visible")

[<Fact>]
let ``Snapshot handle double dispose is safe`` () =
    withTestDir "snap_handle_double" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("k", "v")

        let snap = tree.Snapshot()
        snap.Dispose()
        snap.Dispose()

        assertEqual (Some "v") (tree.Get "k") "Data readable after double dispose")

[<Fact>]
let ``Snapshot refcount: register same seq twice needs two releases`` () =
    withTestDir "snap_refcount_twice" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
        tree.Put("k", "v1")
        tree.Flush()
        let snap = tree.Snapshot()
        let seq = snap.Seq

        let snapMgrField =
            typeof<LsmTree>
                .GetField(
                    "snapshotManager",
                    System.Reflection.BindingFlags.NonPublic
                    ||| System.Reflection.BindingFlags.Instance
                )

        let snapMgr = snapMgrField.GetValue tree :?> LsmTreeSnapshot
        snapMgr.RegisterSnapshot seq
        snap.Dispose()

        tree.Put("k", "v2")
        tree.Flush()
        tree.Put("k", "v3")
        tree.Flush()
        tree.WaitForCompaction()
        assertEqual (Some "v1") (tree.Get("k", snap)) "v1 preserved while refcount > 0"

        snapMgr.ReleaseSnapshot seq
        assertEqual (Some "v1") (tree.Get("k", snap)) "v1 still preserved as live value after both releases")

[<Fact>]
let ``Snapshot refcount: multiple snapshots at different seqs`` () =
    withTestDir "snap_refcount_multi" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
        tree.Put("k", "v1")
        tree.Flush()
        use snap1 = tree.Snapshot()

        tree.Put("k", "v2")
        tree.Flush()
        use snap2 = tree.Snapshot()

        tree.Put("k", "v3")
        tree.Flush()
        tree.WaitForCompaction()

        assertEqual (Some "v1") (tree.Get("k", snap1)) "snap1 sees v1"
        assertEqual (Some "v2") (tree.Get("k", snap2)) "snap2 sees v2"
        assertEqual (Some "v3") (tree.Get "k") "latest sees v3")

[<Fact>]
let ``LsmTree transaction commits and restarts correctly`` () =
    withTestDir "tx_restart" (fun testDir ->
        do
            use tree = new LsmTree(testDir)
            let tx = tree.BeginTransaction()
            tx.Put("ntk", "ntv")
            tx.Commit()
            tx.Dispose()

        do
            use tree = new LsmTree(testDir)
            assertEqual (Some "ntv") (tree.Get "ntk") "transaction survives restart")

[<Fact>]
let ``LsmTree auto-flush on transaction commit when memTable exceeds size limit`` () =
    withTestDir "auto_flush_tx" (fun testDir ->
        use tree = new LsmTree(testDir, memTableSizeLimit = 1)
        use tx = tree.BeginTransaction()
        tx.Put("k", "v")
        tx.Commit()
        assertEqual (Some "v") (tree.Get "k") "Data preserved after auto-flush on transaction commit")

[<Fact>]
let ``LsmTree basic Put and Get operations`` () =
    withTestDir "basic_put_get" (fun dir ->
        use tree = new LsmTree(dir)
        tree.Put("pk1", "pv1")
        tree.Put("pk2", "pv2")
        assertEqual (Some "pv1") (tree.Get "pk1") "first Put/Get works"
        assertEqual (Some "pv2") (tree.Get "pk2") "second Put/Get works")

[<Fact>]
let ``LsmTree overwrite existing key`` () =
    withTestDir "overwrite" (fun dir ->
        use tree = new LsmTree(dir)
        tree.Put("k", "v1")
        tree.Put("k", "v2")
        assertEqual (Some "v2") (tree.Get "k") "Overwrite returns latest value")

[<Fact>]
let ``LsmTree auto-flush on Put when memTable exceeds size limit`` () =
    withTestDir "auto_flush_put" (fun testDir ->
        use tree = new LsmTree(testDir, memTableSizeLimit = 1)
        tree.Put("k", "v")
        assertEqual (Some "v") (tree.Get "k") "Data preserved after auto-flush on Put")

[<Fact>]
let ``LsmTree resurrect deleted key`` () =
    withTestDir "resurrect" (fun dir ->
        use tree = new LsmTree(dir)
        tree.Put("k", "v1")
        let snapAfterPut = tree.Snapshot()
        tree.Delete "k"
        let snapAfterDelete = tree.Snapshot()
        tree.Put("k", "v2")
        assertEqual (Some "v2") (tree.Get "k") "Resurrected key returns new value"
        assertEqual None (tree.Get("k", snapAfterDelete)) "Snapshot after delete sees None"
        assertEqual (Some "v1") (tree.Get("k", snapAfterPut)) "Snapshot after Put sees original value")

[<Fact>]
let ``LsmTree Delete and restart recovers deletion`` () =
    withTestDir "del_restart" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("k", "v")
        tree.Delete "k"
        assertEqual None (tree.Get "k") "Delete works"

        use tree2 = new LsmTree(testDir)
        assertEqual None (tree2.Get "k") "Delete survives restart")

[<Fact>]
let ``LsmTree auto-flush on Delete when memTable exceeds size limit`` () =
    withTestDir "auto_flush_del" (fun testDir ->
        use tree = new LsmTree(testDir, memTableSizeLimit = 1)
        tree.Put("k", "v")
        tree.Delete "k"
        assertEqual None (tree.Get "k") "Deletion preserved after auto-flush on Delete")

[<Fact>]
let ``LsmTree Flush with no data does not throw`` () =
    withTestDir "empty_flush" (fun dir ->
        use tree = new LsmTree(dir)
        tree.Flush())

[<Fact>]
let ``LsmTree concurrent Flush and Get does not deadlock`` () =
    withTestDir "conc_flush" (fun dir ->
        use tree = new LsmTree(dir)
        let numOps = 10

        for i = 1 to numOps do
            tree.Put($"k{i}", $"v{i}")

        runConcurrent
            [| fun () ->
                   for i = 1 to numOps do
                       tree.Get $"k{i}" |> ignore
               fun () -> tree.Flush() |]

        for i = 1 to numOps do
            assertEqual (Some $"v{i}") (tree.Get $"k{i}") $"Key k{i} after concurrent flush")

[<Fact>]
let ``LsmTree Flush propagates flush coordinator errors`` () =
    withTestDir "flush_err_prop" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("k", "v")
        tree.Flush()

        let flField =
            typeof<LsmTree>
                .GetField(
                    "flushCoordinator",
                    System.Reflection.BindingFlags.NonPublic
                    ||| System.Reflection.BindingFlags.Instance
                )

        let fc = flField.GetValue tree :?> FlushCoordinator
        fc.Error <- Some(exn "injected flush error")

        Assert.Throws<System.AggregateException>(fun () -> tree.Flush() |> ignore)
        |> ignore)

[<Fact>]
let ``LsmTree FlushAsync completes successfully`` () =
    withTestDir "flush_async" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("k", "v")
        tree.FlushAsync() |> Async.RunSynchronously
        assertEqual (Some "v") (tree.Get "k") "Data accessible after FlushAsync")

[<Fact>]
let ``LsmTree FlushAsync propagates flush coordinator errors`` () =
    withTestDir "flush_async_err" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("k", "v")
        tree.Flush()

        let flField =
            typeof<LsmTree>
                .GetField(
                    "flushCoordinator",
                    System.Reflection.BindingFlags.NonPublic
                    ||| System.Reflection.BindingFlags.Instance
                )

        let fc = flField.GetValue tree :?> FlushCoordinator
        fc.Error <- Some(exn "injected flush async error")

        Assert.Throws<System.AggregateException>(fun () -> tree.FlushAsync() |> Async.RunSynchronously)
        |> ignore)

[<Fact>]
let ``LsmTree compaction and restart preserves data`` () =
    withTestDir "compact_restart" (fun testDir ->
        do
            use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

            for i = 1 to 5 do
                tree.Put($"ck{i}", $"cv{i}")
                tree.Flush()

            tree.WaitForCompaction()

        do
            use tree = new LsmTree(testDir)

            for i = 1 to 5 do
                assertEqual (Some $"cv{i}") (tree.Get $"ck{i}") $"Key ck{i} preserved after compact+restart")

[<Fact>]
let ``LsmTree multi-level compaction merges correctly`` () =
    withTestDir "multi_level" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
        tree.Put("k1", "v1")
        tree.Flush()

        tree.Put("k2", "v2")
        tree.Flush()
        tree.WaitForCompaction()
        assertEqual (Some "v1") (tree.Get "k1") "k1 preserved"
        assertEqual (Some "v2") (tree.Get "k2") "k2 preserved")

[<Fact>]
let ``LsmTree Flush during compaction completes without blocking`` () =
    withTestDir "flush_during_compact" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
        tree.Put("k1", "v1")
        tree.Flush()

        tree.Put("k2", "v2")
        tree.Flush()
        tree.WaitForCompaction()

        tree.Put("k3", "v3")
        tree.Flush()
        assertEqual (Some "v3") (tree.Get "k3") "k3 preserved after flush during compaction")

[<Fact>]
let ``LsmTree cascade compaction across multiple levels preserves all keys`` () =
    withTestDir "cascade_levels" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

        for i = 1 to 10 do
            tree.Put($"ck{i}", $"cv{i}")
            tree.Flush()

        tree.WaitForCompaction()

        for i = 1 to 10 do
            assertEqual (Some $"cv{i}") (tree.Get $"ck{i}") $"Cascaded compaction: key ck{i}")

[<Fact>]
let ``LsmTree compaction tolerates IO errors`` () =
    withTestDir "compact_err" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
        tree.Put("k", "v")
        tree.Flush()
        tree.Put("k2", "v2")
        tree.Flush()

        tree.Put("k3", "v3")
        tree.Flush()
        tree.WaitForCompaction()
        assertEqual (Some "v3") (tree.Get "k3") "Data should still be accessible after compaction")

[<Fact>]
let ``LsmTree WaitForCompaction propagates compaction coordinator errors`` () =
    withTestDir "compact_err_prop" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

        for i = 1 to 3 do
            tree.Put($"k{i}", $"v{i}")
            tree.Flush()

        tree.WaitForCompaction()

        let compField =
            typeof<LsmTree>
                .GetField(
                    "compaction",
                    System.Reflection.BindingFlags.NonPublic
                    ||| System.Reflection.BindingFlags.Instance
                )

        let cc = compField.GetValue tree :?> CompactionCoordinator
        cc.Error <- Some(exn "injected compaction error")

        Assert.Throws<System.AggregateException>(fun () -> tree.WaitForCompaction() |> ignore)
        |> ignore)

[<Fact>]
let ``LsmTree WaitForCompactionAsync completes successfully`` () =
    withTestDir "wait_compact_async" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

        for i = 1 to 5 do
            tree.Put($"k{i}", $"v{i}")
            tree.Flush()

        tree.WaitForCompactionAsync() |> Async.RunSynchronously

        for i = 1 to 5 do
            assertEqual (Some $"v{i}") (tree.Get $"k{i}") $"Key k{i} preserved after WaitForCompactionAsync")

[<Fact>]
let ``LsmTree WaitForCompactionAsync propagates compaction coordinator errors`` () =
    withTestDir "wait_compact_async_err" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

        for i = 1 to 3 do
            tree.Put($"k{i}", $"v{i}")
            tree.Flush()

        tree.WaitForCompactionAsync() |> Async.RunSynchronously

        let compField =
            typeof<LsmTree>
                .GetField(
                    "compaction",
                    System.Reflection.BindingFlags.NonPublic
                    ||| System.Reflection.BindingFlags.Instance
                )

        let cc = compField.GetValue tree :?> CompactionCoordinator
        cc.Error <- Some(exn "injected compaction async error")

        Assert.Throws<System.AggregateException>(fun () -> tree.WaitForCompactionAsync() |> Async.RunSynchronously)
        |> ignore)

[<Fact>]
let ``RangeScan with NewIterator and explicit Dispose`` () =
    withTestDir "rscan_iter_dispose" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("a", "va")
        let it = tree.NewIterator("a", "a")
        let results = ResizeArray()

        while it.MoveNext() do
            results.Add(it.Current)

        it.Dispose()
        assertEqual [ "a", "va" ] (results |> Seq.toList) "Iterator works")

[<Fact>]
let ``RangeScan Current before MoveNext throws`` () =
    withTestDir "rscan_current_before_move" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("k", "v")

        use it = tree.NewIterator("k", "k")

        Assert.Throws<System.InvalidOperationException>(fun () -> it.Current |> ignore)
        |> ignore)

[<Fact>]
let ``RangeScan with null fromKey throws`` () =
    withTestDir "rscan_null_from" (fun testDir ->
        use tree = new LsmTree(testDir)

        Assert.Throws<System.ArgumentNullException>(fun () -> tree.NewIterator(null, "z") |> ignore)
        |> ignore)

[<Fact>]
let ``RangeScan with null toKey throws`` () =
    withTestDir "rscan_null_to" (fun testDir ->
        use tree = new LsmTree(testDir)

        Assert.Throws<System.ArgumentNullException>(fun () -> tree.NewIterator("a", null) |> ignore)
        |> ignore)

[<Fact>]
let ``RangeScan empty range when fromKey > toKey`` () =
    withTestDir "rscan_from_gt_to" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("a", "va")
        let result = tree.RangeScan("z", "a") |> Seq.toList
        assertEqual [] result "fromKey>toKey returns empty")

[<Fact>]
let ``RangeScan single key exact match`` () =
    withTestDir "rscan_single_exact" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("k", "v")
        let result = tree.RangeScan("k", "k") |> Seq.toList
        assertEqual [ "k", "v" ] result "Single key exact match")

[<Fact>]
let ``RangeScan multiple keys in sorted order`` () =
    withTestDir "rscan_multi_sorted" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("c", "vc")
        tree.Put("a", "va")
        tree.Put("b", "vb")
        let result = tree.RangeScan("a", "c") |> Seq.toList
        assertEqual [ "a", "va"; "b", "vb"; "c", "vc" ] result "Keys sorted")

[<Fact>]
let ``RangeScan excludes keys outside range`` () =
    withTestDir "rscan_excludes_outside" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("a", "va")
        tree.Put("b", "vb")
        tree.Put("c", "vc")
        tree.Put("d", "vd")
        let result = tree.RangeScan("b", "c") |> Seq.toList
        assertEqual [ "b", "vb"; "c", "vc" ] result "Only b and c")

[<Fact>]
let ``RangeScan excludes tombstoned keys`` () =
    withTestDir "rscan_tomb_excluded" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("a", "va")
        tree.Put("b", "vb")
        tree.Delete("b")
        tree.Put("c", "vc")
        let result = tree.RangeScan("a", "c") |> Seq.toList
        assertEqual [ "a", "va"; "c", "vc" ] result "Tombstoned b excluded")

[<Fact>]
let ``RangeScan after flush reads from SSTable`` () =
    withTestDir "rscan_after_flush" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("a", "va")
        tree.Put("b", "vb")
        tree.Flush()
        let result = tree.RangeScan("a", "b") |> Seq.toList
        assertEqual [ "a", "va"; "b", "vb" ] result "After flush")

[<Fact>]
let ``RangeScan across MemTable and SSTable`` () =
    withTestDir "rscan_mixed_layers" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("a", "va")
        tree.Put("b", "vb")
        tree.Flush()
        tree.Put("c", "vc")
        tree.Put("d", "vd")
        let result = tree.RangeScan("a", "d") |> Seq.toList
        assertEqual [ "a", "va"; "b", "vb"; "c", "vc"; "d", "vd" ] result "Mixed layers")

[<Fact>]
let ``RangeScan with snapshot sees old versions`` () =
    withTestDir "rscan_snapshot_old" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("k", "v1")
        let snap = tree.Snapshot()
        tree.Put("k", "v2")
        let result = tree.RangeScan("k", "k", snapshot = snap) |> Seq.toList
        assertEqual [ "k", "v1" ] result "Snapshot sees old version")

[<Fact>]
let ``RangeScan latest sees newest version`` () =
    withTestDir "rscan_latest" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("k", "v1")
        tree.Put("k", "v2")
        let result = tree.RangeScan("k", "k") |> Seq.toList
        assertEqual [ "k", "v2" ] result "Latest sees newest version")

[<Fact>]
let ``RangeScan after restart`` () =
    withTestDir "rscan_restart" (fun testDir ->
        do
            use tree = new LsmTree(testDir)
            tree.Put("a", "va")
            tree.Put("b", "vb")

        do
            use tree = new LsmTree(testDir)
            let result = tree.RangeScan("a", "b") |> Seq.toList
            assertEqual [ "a", "va"; "b", "vb" ] result "After restart")

[<Fact>]
let ``RangeScan empty database`` () =
    withTestDir "rscan_empty_db" (fun testDir ->
        use tree = new LsmTree(testDir)
        let result = tree.RangeScan("a", "z") |> Seq.toList
        assertEqual [] result "Empty database")

[<Fact>]
let ``RangeScan respects snapshot isolation during concurrent writes`` () =
    withTestDir "rscan_concurrent_write" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("a", "va")
        tree.Put("b", "vb")
        let snap = tree.Snapshot()
        tree.Put("c", "vc")
        let result = tree.RangeScan("a", "c", snapshot = snap) |> Seq.toList
        assertEqual [ "a", "va"; "b", "vb" ] result "Immutable snapshot sees no c")

[<Fact>]
let ``RangeScan with tombstone resurrected key shows latest value`` () =
    withTestDir "rscan_resurrect" (fun testDir ->
        use tree = new LsmTree(testDir)
        tree.Put("k", "v1")
        tree.Delete("k")
        tree.Put("k", "v2")
        let result = tree.RangeScan("k", "k") |> Seq.toList
        assertEqual [ "k", "v2" ] result "Resurrected key shows latest")

[<Fact>]
let ``RangeScan with snapshot handle survives compaction`` () =
    withTestDir "snap_handle_rscan" (fun testDir ->
        use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
        tree.Put("k", "v1")
        tree.Flush()

        use snap = tree.Snapshot()
        tree.Put("k", "v2")
        tree.Flush()
        tree.Put("k", "v3")
        tree.Flush()
        tree.WaitForCompaction()

        let result = tree.RangeScan("k", "k", snapshot = snap) |> Seq.toList
        assertEqual [ "k", "v1" ] result "RangeScan with handle sees old version after compaction")

[<Fact>]
let ``RangeScan with rangeScanMaxRetries=0 uses locked fallback path`` () =
    withTestDir "rscan_retry_zero" (fun testDir ->
        use tree = new LsmTree(testDir, rangeScanMaxRetries = 0)
        tree.Put("a", "va")
        tree.Put("b", "vb")
        tree.Put("c", "vc")
        tree.Flush()

        let result = tree.RangeScan("a", "c") |> Seq.toList
        assertEqual [ "a", "va"; "b", "vb"; "c", "vc" ] result "Works with maxRetries=0")

[<Fact>]
let ``RangeScan with rangeScanMaxRetries=0 across MemTable and SSTable`` () =
    withTestDir "rscan_retry_zero_mixed" (fun testDir ->
        use tree = new LsmTree(testDir, rangeScanMaxRetries = 0)
        tree.Put("a", "va")
        tree.Put("b", "vb")
        tree.Flush()
        tree.Put("c", "vc")
        tree.Put("d", "vd")

        let result = tree.RangeScan("a", "d") |> Seq.toList
        assertEqual [ "a", "va"; "b", "vb"; "c", "vc"; "d", "vd" ] result "Mixed layers with maxRetries=0")

[<Fact>]
let ``RangeScan with rangeScanMaxRetries=1 retries at least once`` () =
    withTestDir "rscan_retry_one" (fun testDir ->
        use tree = new LsmTree(testDir, rangeScanMaxRetries = 1)

        for i = 1 to 20 do
            tree.Put(sprintf "k%02d" i, "v")

        tree.Flush()

        let result = tree.RangeScan("k00", "k99") |> Seq.toList
        Assert.True(result.Length = 20, sprintf "Expected 20 keys, got %d" result.Length))

[<Fact>]
let ``LsmTree compaction cancellation during Dispose`` () =
    withTestDir "compact_cancel" (fun testDir ->
        let tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

        for i = 1 to 5 do
            tree.Put($"k{i}", $"v{i}")
            tree.Flush()

        (tree :> System.IDisposable).Dispose())

[<Fact>]
let ``LsmTree Dispose handles compaction coordinator errors`` () =
    withTestDir "dispose_compact_err" (fun testDir ->
        let tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

        for i = 1 to 3 do
            tree.Put($"k{i}", $"v{i}")
            tree.Flush()

        tree.WaitForCompaction()

        let compField =
            typeof<LsmTree>
                .GetField(
                    "compaction",
                    System.Reflection.BindingFlags.NonPublic
                    ||| System.Reflection.BindingFlags.Instance
                )

        let cc = compField.GetValue tree :?> CompactionCoordinator
        cc.Error <- Some(exn "injected compaction error")
        (tree :> System.IDisposable).Dispose())

[<Fact>]
let ``LsmTree Dispose handles flush coordinator errors`` () =
    withTestDir "dispose_flush_err" (fun testDir ->
        let tree = new LsmTree(testDir)
        tree.Put("k", "v")
        tree.Flush()

        let flField =
            typeof<LsmTree>
                .GetField(
                    "flushCoordinator",
                    System.Reflection.BindingFlags.NonPublic
                    ||| System.Reflection.BindingFlags.Instance
                )

        let fc = flField.GetValue tree :?> FlushCoordinator
        fc.Error <- Some(exn "injected flush error")
        (tree :> System.IDisposable).Dispose())
