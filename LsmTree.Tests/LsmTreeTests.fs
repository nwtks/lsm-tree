module LsmTree.Tests.LsmTreeTests

open Xunit
open LsmTree

[<Fact>]
let ``Put and get from MemTable works correctly`` () =
    let testDataDir = getTestDir "1"
    use tree = new LsmTree(testDataDir)
    tree.Put("k1", "v1")
    assertEqual (Some "v1") (tree.Get "k1") "k1 should be v1"
    assertEqual None (tree.Get "k2") "k2 should be None"

    tree.Close()

[<Fact>]
let ``Overwriting a key multiple times keeps the latest value`` () =
    let testDataDir = getTestDir "overwrite"
    use tree = new LsmTree(testDataDir)
    tree.Put("k", "v1")
    tree.Put("k", "v2")
    tree.Put("k", "v3")
    assertEqual (Some "v3") (tree.Get "k") "Should see the latest version"

    tree.Flush()
    assertEqual (Some "v3") (tree.Get "k") "Should see the latest version after flush"

    tree.Close()

[<Fact>]
let ``Delete marks a key with a tombstone so Get returns None`` () =
    let testDataDir = getTestDir "2"
    use tree = new LsmTree(testDataDir)
    tree.Put("k1", "v1")
    tree.Delete "k1"
    assertEqual None (tree.Get "k1") "k1 should be deleted (Tombstone applied)"

    tree.Close()

[<Fact>]
let ``Deleting a non-existent key returns None`` () =
    let testDataDir = getTestDir "del_none"
    use tree = new LsmTree(testDataDir)
    tree.Delete "no_such_key"
    assertEqual None (tree.Get "no_such_key") "Deleting non-existent key should be a no-op/tombstone but result in None"

    tree.Close()

[<Fact>]
let ``Flush to SSTable and read back`` () =
    let testDataDir = getTestDir "3"
    use tree = new LsmTree(testDataDir, 10)
    tree.Put("key1", "value1")
    tree.Put("key2", "value2")
    tree.Flush()
    assertEqual (Some "value1") (tree.Get "key1") "Should read key1 from flushed SSTable"
    assertEqual (Some "value2") (tree.Get "key2") "Should read key2 from flushed SSTable"
    assertEqual None (tree.Get "definitely_not_there") "Search should safely reach end and return None"

    tree.Close()

[<Fact>]
let ``Multi-level compaction from L0 to L1`` () =
    let testDataDir = getTestDir "6"
    use tree = new LsmTree(testDataDir, 10)

    for i = 1 to 5 do
        tree.Put($"c_k{i}", $"c_v{i}")
        tree.Flush()

    tree.WaitForCompaction()

    for i = 1 to 5 do
        assertEqual (Some $"c_v{i}") (tree.Get $"c_k{i}") "Compacted keys must be readable"

    let l1Files = System.IO.Directory.GetFiles(testDataDir, "L1_*.sst")
    Assert.True(l1Files.Length = 1, $"Expected 1 compacted L1 file, but found {l1Files.Length}")

[<Fact>]
let ``Snapshot pruning removes old versions correctly`` () =
    let testDataDir = getTestDir "pruning"
    use tree = new LsmTree(testDataDir, 10)
    tree.Put("kp", "v1")
    tree.Flush()
    tree.Put("kp", "v2")
    tree.Flush()
    tree.Put("kp", "v3")
    tree.Flush()
    tree.WaitForCompaction()

    tree.Put("other", "data")
    tree.Flush()
    tree.Put("other2", "data")
    tree.Flush()
    tree.Put("other3", "data")
    tree.Flush()
    tree.Put("other4", "data")
    tree.Flush()
    tree.WaitForCompaction()
    assertEqual (Some "v3") (tree.Get "kp") "Current should be v3"

[<Fact>]
let ``Merge SSTables during compaction`` () =
    let testDataDir = getTestDir "merge_cov"
    let limits = [| 1; 1 |]
    use tree = new LsmTree(testDataDir, 1, compactLevelLimits = limits)
    tree.Put("km", "v1")
    tree.Flush()
    tree.Put("km", "v2")
    tree.Flush()
    tree.Put("km", "v3")
    tree.Flush()
    tree.WaitForCompaction()
    tree.Delete "kd"
    tree.Flush()

    for i = 1 to 10 do
        tree.Put($"other_{i}", "data")
        tree.Flush()

    tree.WaitForCompaction()
    assertEqual (Some "v3") (tree.Get "km") "Current km = v3"
    assertEqual None (tree.Get "kd") "kd is deleted"

[<Fact>]
let ``MVCC provides multi-version concurrency control across snapshots`` () =
    let testDataDir = getTestDir "7"
    use tree = new LsmTree(testDataDir)
    tree.Put("mvcc_key", "version1")
    let snap1 = tree.Snapshot()
    tree.Put("mvcc_key", "version2")
    let snap2 = tree.Snapshot()
    tree.Put("mvcc_key", "version3")
    let snap3 = tree.Snapshot()
    tree.Delete "mvcc_key"
    assertEqual None (tree.Get "mvcc_key") "Current timeline should have key deleted"
    assertEqual (Some "version3") (tree.Get("mvcc_key", snap3)) "Snapshot 3 should read version 3"
    assertEqual (Some "version2") (tree.Get("mvcc_key", snap2)) "Snapshot 2 should read version 2"
    assertEqual (Some "version1") (tree.Get("mvcc_key", snap1)) "Snapshot 1 should read version 1"

    tree.Flush()
    tree.WaitForCompaction()
    assertEqual None (tree.Get "mvcc_key") "Post-flush: Current timeline should have key deleted"
    assertEqual (Some "version3") (tree.Get("mvcc_key", snap3)) "Post-flush: Snapshot 3 should read version 3"
    assertEqual (Some "version2") (tree.Get("mvcc_key", snap2)) "Post-flush: Snapshot 2 should read version 2"
    assertEqual (Some "version1") (tree.Get("mvcc_key", snap1)) "Post-flush: Snapshot 1 should read version 1"

[<Fact>]
let ``LsmTree startup creates the data directory`` () =
    let testDataDir =
        System.IO.Path.Combine(System.Environment.CurrentDirectory, "test_data_new_dir")

    if System.IO.Directory.Exists testDataDir then
        System.IO.Directory.Delete(testDataDir, true)

    try
        use tree = new LsmTree(testDataDir)
        tree.Put("k1", "v1")
        Assert.True(System.IO.Directory.Exists testDataDir, "Directory should be created")
    finally
        if System.IO.Directory.Exists testDataDir then
            System.IO.Directory.Delete(testDataDir, true)

[<Fact>]
let ``LsmTree restart loads data from SSTable and WAL`` () =
    let testDataDir = getTestDir "restart_load"

    do
        use tree = new LsmTree(testDataDir, 100)
        tree.Put("k1", "v1")
        tree.Flush()
        tree.Put("k2", "v2")

    use tree2 = new LsmTree(testDataDir)
    assertEqual (Some "v1") (tree2.Get "k1") "Should load from SSTable"
    assertEqual (Some "v2") (tree2.Get "k2") "Should load from WAL"

[<Fact>]
let ``Concurrent flush and get on immutable MemTable does not crash`` () =
    let testDataDir = getTestDir "imm_race"
    use tree = new LsmTree(testDataDir, 1000)
    tree.Put("race_k", "race_v")

    let tasks =
        [| for _ = 1 to 10 do
               yield
                   System.Threading.Tasks.Task.Run(fun () ->
                       for _ = 1 to 10 do
                           tree.Flush()
                           tree.Get "race_k" |> ignore
                           tree.Put("race_k", "race_v")
                           tree.Get "race_k" |> ignore
                           tree.Delete "race_k"
                           tree.Get "race_k" |> ignore) |]

    System.Threading.Tasks.Task.WaitAll tasks
    Assert.True(true, "Should not crash during concurrent flush/get")

[<Fact>]
let ``Flush works correctly while compaction is running in background`` () =
    let testDataDir = getTestDir "flush_during_compact"
    use tree = new LsmTree(testDataDir, 50)
    tree.Put("k1", "v1")
    tree.Flush()
    tree.Put("k2", "v2")
    tree.Flush()

    for batch = 1 to 4 do
        for i = 1 to 5 do
            tree.Put($"ck_{batch}_{i}", $"cv_{batch}_{i}")

        tree.Flush()

    tree.WaitForCompaction()

    assertEqual (Some "v1") (tree.Get "k1") "k1 should survive concurrent flush + compaction"
    assertEqual (Some "v2") (tree.Get "k2") "k2 should survive concurrent flush + compaction"
    assertEqual (Some "cv_2_3") (tree.Get "ck_2_3") "Concurrent flush data should survive"
    assertEqual None (tree.Get "nonexistent") "Non-existent key should return None"

[<Fact>]
let ``Compaction cascades through multiple levels when limits exceeded`` () =
    let testDataDir = getTestDir "cascade_levels"
    let limits = [| 1; 2; 3 |]
    use tree = new LsmTree(testDataDir, 1024 * 1024, compactLevelLimits = limits)

    let totalRounds = 6

    for round = 0 to totalRounds - 1 do
        for i = 0 to 3 do
            tree.Put($"k{round}_{i}", $"v{round}_{i}")
            tree.Flush()

        tree.WaitForCompaction()

    for round = 0 to totalRounds - 1 do
        for i = 0 to 3 do
            assertEqual (Some $"v{round}_{i}") (tree.Get $"k{round}_{i}") $"k{round}_{i} should survive cascade"

[<Fact>]
let ``Compaction handles last-level tombstone pruning gracefully`` () =
    let testDataDir = getTestDir "compact_prune_last"
    let limits = [| 1; 1 |]
    use tree = new LsmTree(testDataDir, 50, compactLevelLimits = limits)

    for i = 1 to 4 do
        tree.Put($"k{i}", $"v{i}")
        tree.Delete $"k{i}"
        tree.Flush()
        tree.WaitForCompaction()

    for i = 1 to 4 do
        assertEqual None (tree.Get $"k{i}") $"k{i} should be gone (tombstone pruned)"

    tree.Put("fresh", "data")
    assertEqual (Some "data") (tree.Get "fresh") "Engine should work after tombstone pruning"

[<Fact>]
let ``syncOnCommit=false works for basic operations`` () =
    let testDataDir = getTestDir "nosync_crud"
    use tree = new LsmTree(testDataDir, syncOnCommit = false)
    Assert.False(tree.SyncOnCommit, "SyncOnCommit property should be false")

    tree.Put("k1", "v1")
    tree.Put("k2", "v2")
    assertEqual (Some "v1") (tree.Get "k1") "Should read k1 with syncOnCommit=false"
    assertEqual (Some "v2") (tree.Get "k2") "Should read k2 with syncOnCommit=false"

    tree.Delete "k1"
    assertEqual None (tree.Get "k1") "k1 should be deleted with syncOnCommit=false"
    assertEqual (Some "v2") (tree.Get "k2") "k2 should remain after delete"

[<Fact>]
let ``syncOnCommit=false data survives clean close and reopen`` () =
    let testDataDir = getTestDir "nosync_restart"

    do
        use tree = new LsmTree(testDataDir, syncOnCommit = false)
        tree.Put("persist_k1", "persist_v1")
        tree.Put("persist_k2", "persist_v2")

    use tree2 = new LsmTree(testDataDir)
    assertEqual (Some "persist_v1") (tree2.Get "persist_k1") "k1 should survive clean restart with syncOnCommit=false"
    assertEqual (Some "persist_v2") (tree2.Get "persist_k2") "k2 should survive clean restart with syncOnCommit=false"

[<Fact>]
let ``syncOnCommit=false works with transactions`` () =
    let testDataDir = getTestDir "nosync_tx"

    do
        use tree = new LsmTree(testDataDir, syncOnCommit = false)
        use tx = tree.BeginTransaction()
        tx.Put("tx_k", "tx_v")
        tx.Commit()

        assertEqual (Some "tx_v") (tree.Get "tx_k") "Should read committed transaction data with syncOnCommit=false"

    use tree2 = new LsmTree(testDataDir)
    assertEqual (Some "tx_v") (tree2.Get "tx_k") "Transaction data should survive restart with syncOnCommit=false"

[<Fact>]
let ``syncOnCommit=false with flush and compaction`` () =
    let testDataDir = getTestDir "nosync_compact"
    let limits = [| 1 |]

    do
        use tree = new LsmTree(testDataDir, 500, false, limits)
        Assert.False(tree.SyncOnCommit, "SyncOnCommit property should be false")

        for i = 1 to 5 do
            tree.Put($"c_k{i}", $"c_v{i}")

        tree.Flush()
        tree.WaitForCompaction()

        for i = 1 to 5 do
            assertEqual
                (Some $"c_v{i}")
                (tree.Get $"c_k{i}")
                $"k{i} should be readable after compact with syncOnCommit=false"

    let sstFiles = System.IO.Directory.GetFiles(testDataDir, "*.sst")
    Assert.True(sstFiles.Length >= 1, $"Expected ≥1 SSTable, got {sstFiles.Length}")

    use tree2 = new LsmTree(testDataDir)

    for i = 1 to 5 do
        assertEqual
            (Some $"c_v{i}")
            (tree2.Get $"c_k{i}")
            $"k{i} should survive restart after compact with syncOnCommit=false"

[<Fact>]
let ``Compaction error propagrates through WaitForCompaction`` () =
    let testDataDir = getTestDir "compact_error_prop"

    do
        use tree = new LsmTree(testDataDir, 500, compactLevelLimits = [| 100; 100; 100 |])

        for i = 1 to 10 do
            tree.Put($"k{i}", $"v{i}")
            tree.Flush()

    do
        use tree = new LsmTree(testDataDir, 500, compactLevelLimits = [| 0; 0; 1 |])

        let ssTablesField =
            typeof<LsmTree>
                .GetField(
                    "ssTables",
                    System.Reflection.BindingFlags.NonPublic
                    ||| System.Reflection.BindingFlags.Instance
                )

        let ssTables = ssTablesField.GetValue tree :?> SSTable list[]

        let fsField =
            typeof<SSTable>
                .GetField(
                    "fs",
                    System.Reflection.BindingFlags.NonPublic
                    ||| System.Reflection.BindingFlags.Instance
                )

        for sst in ssTables.[0] do
            let fs = fsField.GetValue sst :?> System.IO.FileStream
            fs.Close()

        tree.Put("trigger", "compact")
        tree.Flush()

        let ex =
            Assert.Throws<System.AggregateException>(fun () -> tree.WaitForCompaction())

        Assert.Contains("Compaction failed", ex.Message)
        Assert.NotNull ex.InnerException
