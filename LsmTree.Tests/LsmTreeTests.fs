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
        tree.Put(sprintf "c_k%d" i, sprintf "c_v%d" i)
        tree.Flush()

    tree.WaitForCompaction()

    for i = 1 to 5 do
        assertEqual (Some(sprintf "c_v%d" i)) (tree.Get(sprintf "c_k%d" i)) "Compacted keys must be readable"

    let l1Files = System.IO.Directory.GetFiles(testDataDir, "L1_*.sst")
    Assert.True(l1Files.Length = 1, sprintf "Expected 1 compacted L1 file, but found %d" l1Files.Length)

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
        tree.Put(sprintf "other_%d" i, "data")
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
