module LsmTree.Tests.Tests

open System
open System.IO
open System.Threading.Tasks
open Xunit
open LsmTree

let getTestDir name =
    let dir = Path.Combine(Environment.CurrentDirectory, "test_data_" + name)

    if Directory.Exists dir then
        Directory.Delete(dir, true)

    Directory.CreateDirectory dir |> ignore
    dir

let assertEqual expected actual msg =
    Assert.True((expected = actual), sprintf "%s\n  Expected: %A\n  Actual: %A" msg expected actual)

// ============================================================
// Basic CRUD Operations
// ============================================================

[<Fact>]
let ``Put_and_Get_from_MemTable`` () =
    let testDataDir = getTestDir "1"
    use tree = new LsmTree(testDataDir)
    tree.Put("k1", "v1")
    assertEqual (Some "v1") (tree.Get "k1") "k1 should be v1"
    assertEqual None (tree.Get "k2") "k2 should be None"

    tree.Close()

[<Fact>]
let ``Overwrite_Key_Multiple_Times`` () =
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
let ``Delete_a_key_Tombstone`` () =
    let testDataDir = getTestDir "2"
    use tree = new LsmTree(testDataDir)
    tree.Put("k1", "v1")
    tree.Delete "k1"
    assertEqual None (tree.Get "k1") "k1 should be deleted (Tombstone applied)"

    tree.Close()

[<Fact>]
let ``Delete_NonExistent_Key`` () =
    let testDataDir = getTestDir "del_none"
    use tree = new LsmTree(testDataDir)
    tree.Delete "no_such_key"
    assertEqual None (tree.Get "no_such_key") "Deleting non-existent key should be a no-op/tombstone but result in None"

    tree.Close()

// ============================================================
// SSTable, Flush & Compaction
// ============================================================

[<Fact>]
let ``Flush_to_SSTable_and_Read`` () =
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
let ``Multi_Level_Compaction_L0_L1`` () =
    let testDataDir = getTestDir "6"
    use tree = new LsmTree(testDataDir, 10)

    for i = 1 to 5 do
        tree.Put(sprintf "c_k%d" i, sprintf "c_v%d" i)
        tree.Flush()

    tree.WaitForCompaction()

    for i = 1 to 5 do
        assertEqual (Some(sprintf "c_v%d" i)) (tree.Get(sprintf "c_k%d" i)) "Compacted keys must be readable"

    let l1Files = Directory.GetFiles(testDataDir, "L1_*.sst")
    Assert.True(l1Files.Length = 1, sprintf "Expected 1 compacted L1 file, but found %d" l1Files.Length)

[<Fact>]
let ``Snapshot_Pruning_Verification`` () =
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
let ``Test_MergeSSTables_Coverage`` () =
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

// ============================================================
// SSTable Internals
// ============================================================

[<Fact>]
let ``SSTable_Level_Parsing_and_Recovery_Ordering`` () =
    let testDataDir = getTestDir "sst_levels"
    let l1Path = Path.Combine(testDataDir, "L1_data.sst")
    let l0Path = Path.Combine(testDataDir, "L0_data.sst")
    let legacyPath = Path.Combine(testDataDir, "legacy.sst")
    SSTableWriter.write l1Path [ "k1", 1L, Some "v1_L1" ]
    SSTableWriter.write l0Path [ "k1", 200L, Some "v1_L0" ]
    SSTableWriter.write legacyPath [ "k9", 10L, Some "v9" ]

    use tree = new LsmTree(testDataDir)

    assertEqual
        (Some "v1_L0")
        (tree.Get("k1", 300L))
        "Should prefer L0 over L1 (using high snapshot for manual recovery)"

    assertEqual (Some "v9") (tree.Get("k9", 300L)) "legacy.sst should be at level 0"

[<Fact>]
let ``SSTable_Double_Dispose`` () =
    let testDataDir = getTestDir "sst_double_dispose"
    let sstPath = Path.Combine(testDataDir, "double_dispose.sst")
    SSTableWriter.flush sstPath [] |> ignore

    use sst = new SSTable(sstPath)
    (sst :> IDisposable).Dispose()

    (sst :> IDisposable).Dispose()
    Assert.True(true, "Should not throw")

[<Fact>]
let ``SSTable_Load_Short_File_Handling`` () =
    let testDataDir = getTestDir "sst_short"
    let sstPath = Path.Combine(testDataDir, "L0_short.sst")
    File.WriteAllBytes(sstPath, [| 1uy; 2uy; 3uy |])

    use sst = new SSTable(sstPath)
    assertEqual None (sst.Get("any", 0L)) "Should handle short/invalid SSTable file gracefully"

[<Fact>]
let ``SSTable_Invalid_Magic`` () =
    let testDataDir = getTestDir "sst_bad_magic"
    let sstPath = Path.Combine(testDataDir, "bad.sst")

    do
        use fs = new FileStream(sstPath, FileMode.Create, FileAccess.Write)
        use bw = new BinaryWriter(fs)
        bw.Write [| 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy |] // data
        bw.Write 0L // index offset
        bw.Write 0L // bloom offset
        bw.Write 0xFEEDFACEL // bad magic

    Assert.Throws<InvalidDataException>(fun () -> new SSTable(sstPath) |> ignore)

// ============================================================
// MVCC (Multi-Version Concurrency Control)
// ============================================================

[<Fact>]
let ``MVCC_Multi_Version_Concurrency_Control`` () =
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

// ============================================================
// Transactions
// ============================================================

[<Fact>]
let ``Transaction_Read_Own_Writes`` () =
    let testDataDir = getTestDir "tx3"
    use tree = new LsmTree(testDataDir)
    use tx = tree.BeginTransaction()
    tx.Put("tx_k3", "tx_v3")
    assertEqual (Some "tx_v3") (tx.Get "tx_k3") "Should see its own uncommitted write"

    tx.Delete "tx_k3"
    assertEqual None (tx.Get "tx_k3") "Should see its own delete"

[<Fact>]
let ``Transaction_Commit_Visibility`` () =
    let testDataDir = getTestDir "tx1"
    use tree = new LsmTree(testDataDir)
    use tx = tree.BeginTransaction()
    tx.Put("tx_k1", "tx_v1")
    assertEqual None (tree.Get "tx_k1") "Should not see uncommitted write"

    tx.Commit()
    assertEqual (Some "tx_v1") (tree.Get "tx_k1") "Should see committed write"

[<Fact>]
let ``Transaction_Single_Sequence_Commit`` () =
    let testDataDir = getTestDir "tx5"
    use tree = new LsmTree(testDataDir)
    use tx = tree.BeginTransaction()
    tx.Put("k1", "v1")
    tx.Put("k2", "v2")
    tx.Commit()

    use tree2 = new LsmTree(testDataDir)
    let snap = tree2.Snapshot()
    assertEqual 1L snap "Both writes should share sequence 1"
    assertEqual (Some "v1") (tree2.Get "k1") "k1 should be v1"
    assertEqual (Some "v2") (tree2.Get "k2") "k2 should be v2"

[<Fact>]
let ``Transaction_EmptyCommit`` () =
    let testDataDir = getTestDir "tx_empty"
    use tree = new LsmTree(testDataDir)
    use tx = tree.BeginTransaction()
    tx.Commit()
    assertEqual None (tree.Get "any") "Empty commit should work safely"

[<Fact>]
let ``Transaction_Rollback_Visibility`` () =
    let testDataDir = getTestDir "tx_rollback"
    use tree = new LsmTree(testDataDir)
    tree.Put("k1", "v1")
    use tx = tree.BeginTransaction()
    tx.Put("k1", "v2")
    assertEqual (Some "v2") (tx.Get "k1") "Transaction should see local write"

    tx.Rollback()
    assertEqual (Some "v1") (tree.Get "k1") "Database should remain v1 after rollback"

[<Fact>]
let ``Transaction_Snapshot_Isolation`` () =
    let testDataDir = getTestDir "tx4"
    use tree = new LsmTree(testDataDir)
    tree.Put("k", "v1")
    use tx = tree.BeginTransaction()
    tree.Put("k", "v2")
    assertEqual (Some "v1") (tx.Get "k") "Transaction should see the snapshot at its start"

    tx.Commit()
    assertEqual (Some "v2") (tree.Get "k") "Final value should be v2"

[<Fact>]
let ``Transaction_Isolation_Across_Flush`` () =
    let testDataDir = getTestDir "tx_flush"
    use tree = new LsmTree(testDataDir, 1024)
    tree.Put("k1", "initial")
    use tx = tree.BeginTransaction()
    tree.Put("k1", "updated")
    tree.Flush()
    assertEqual (Some "initial") (tx.Get "k1") "Transaction must see its snapshot even after background flush"

[<Fact>]
let ``Transaction_DoubleDispose`` () =
    let testDataDir = getTestDir "tx_double_dispose"
    use tree = new LsmTree(testDataDir)
    let tx = tree.BeginTransaction()
    (tx :> IDisposable).Dispose()

    (tx :> IDisposable).Dispose()
    Assert.True(true, "Should not throw")

[<Fact>]
let ``Transaction_Already_Finished_Errors`` () =
    let testDataDir = getTestDir "tx_errors"
    use tree = new LsmTree(testDataDir)
    use tx = tree.BeginTransaction()
    tx.Commit()

    Assert.Throws<Exception>(fun () -> tx.Put("k", "v") |> ignore) |> ignore
    Assert.Throws<Exception>(fun () -> tx.Delete "k" |> ignore) |> ignore
    Assert.Throws<Exception>(fun () -> tx.Commit() |> ignore) |> ignore
    Assert.Throws<Exception>(fun () -> tx.Rollback() |> ignore) |> ignore

// ============================================================
// WAL (Write-Ahead Log) & Recovery
// ============================================================

[<Fact>]
let ``Auto_recovery_from_WAL`` () =
    let testDataDir = getTestDir "4"
    use tree1 = new LsmTree(testDataDir)
    tree1.Put("wal_key1", "wal_val1")
    tree1.Put("wal_key2", "wal_val2")
    tree1.Delete "wal_key1"

    use tree2 = new LsmTree(testDataDir)
    assertEqual None (tree2.Get "wal_key1") "wal_key1 should be deleted after recovery"
    assertEqual (Some "wal_val2") (tree2.Get "wal_key2") "wal_key2 should be recovered from WAL log"

[<Fact>]
let ``WAL_Atomic_Recovery`` () =
    let testDataDir = getTestDir "tx_wal_atomicity"
    let walPath = Path.Combine(testDataDir, "wal.log")

    do
        use sw = new StreamWriter(walPath)
        sw.WriteLine "BEGIN 1"
        let k1 = WALRecovery.utf8ToBase64 "k1"
        let v1 = WALRecovery.utf8ToBase64 "v1"
        sw.WriteLine(sprintf "PUT 1 %s %s" k1 v1)

    use tree = new LsmTree(testDataDir)
    assertEqual None (tree.Get "k1") "Should not recover k1 because transaction was not committed"

    do
        use sw2 = File.AppendText walPath
        sw2.WriteLine "COMMIT 1"

    use tree2 = new LsmTree(testDataDir)
    assertEqual (Some "v1") (tree2.Get "k1") "Should recover k1 after COMMIT marker is present"

[<Fact>]
let ``WAL_Recovery_Uncommitted_Transaction`` () =
    let testDataDir = getTestDir "tx_wal_uncommitted"
    let walPath = Path.Combine(testDataDir, "wal.log")

    do
        use writer = new StreamWriter(walPath)
        let k = WALRecovery.utf8ToBase64 "k_uncommitted"
        let v = WALRecovery.utf8ToBase64 "v_uncommitted"
        writer.WriteLine(sprintf "%s %d" WALRecovery.BEGIN 100L)
        writer.WriteLine(sprintf "%s %d %s %s" WALRecovery.PUT 100L k v)

    use tree = new LsmTree(testDataDir)
    assertEqual None (tree.Get "k_uncommitted") "Uncommitted transaction should NOT be recovered"

[<Fact>]
let ``WAL_Recover_NonExistent_File`` () =
    let ops = WALRecovery.recover "/tmp/non_existent_wal_path_xyz" |> Seq.toList
    assertEqual [] ops "Recovering non-existent file path"

[<Fact>]
let ``WAL_Recovery_Ignores_Unknown_Entries`` () =
    let testDataDir = getTestDir "wal_edge"
    let walPath = Path.Combine(testDataDir, "wal.log")
    let k = WALRecovery.utf8ToBase64 "k"
    let v = WALRecovery.utf8ToBase64 "v"
    File.WriteAllLines(walPath, [ "UNKNOWN 1 some data"; "BEGIN 2"; sprintf "PUT 2 %s %s" k v; "COMMIT 2" ])

    use tree = new LsmTree(testDataDir)
    assertEqual (Some "v") (tree.Get "k") "Should recover valid transaction even if unknown entry present"

[<Fact>]
let ``WAL_Recovery_Orphaned_Ops`` () =
    let testDataDir = getTestDir "wal_edge_orphan"
    let walPath = Path.Combine(testDataDir, "wal.log")
    let k_orphan = WALRecovery.utf8ToBase64 "key_orphan"
    let v_orphan = WALRecovery.utf8ToBase64 "val_orphan"
    File.WriteAllLines(walPath, [ sprintf "PUT 3 %s %s" k_orphan v_orphan ])

    use tree = new LsmTree(testDataDir)
    assertEqual (Some "val_orphan") (tree.Get "key_orphan") "Orphaned PUT without BEGIN should be recovered"

[<Fact>]
let ``WAL_Recovery_Orphaned_Commit`` () =
    let testDataDir = getTestDir "wal_edge_orphan_commit"
    let walPath = Path.Combine(testDataDir, "wal.log")
    File.WriteAllLines(walPath, [ "COMMIT 4" ])

    use tree = new LsmTree(testDataDir)
    assertEqual None (tree.Get "non_existent") "Orphaned COMMIT with no matching BEGIN should not crash"

[<Fact>]
let ``WAL_Recovery_Ignores_Malformed_Lines`` () =
    let testDataDir = getTestDir "wal_malformed"
    let walPath = Path.Combine(testDataDir, "wal.log")
    // Empty lines, non-numeric seq, too-few fields, and unknown verbs should all be skipped
    let lines = [ ""; "invalid"; "PUT abc k v"; "UNKNOWN 1 k v" ]
    File.WriteAllLines(walPath, lines)

    let ops = WALRecovery.recover walPath |> Seq.toList
    assertEqual [] ops "Should ignore invalid WAL entries"


// ============================================================
// LsmTree Lifecycle & Configuration
// ============================================================

[<Fact>]
let ``LsmTree_Startup_CreatesDirectory`` () =
    let testDataDir = Path.Combine(Environment.CurrentDirectory, "test_data_new_dir")

    if Directory.Exists testDataDir then
        Directory.Delete(testDataDir, true)

    try
        use tree = new LsmTree(testDataDir)
        tree.Put("k1", "v1")
        Assert.True(Directory.Exists testDataDir, "Directory should be created")
    finally
        if Directory.Exists testDataDir then
            Directory.Delete(testDataDir, true)

[<Fact>]
let ``LsmTree_Restart_LoadsData`` () =
    let testDataDir = getTestDir "restart_load"

    do
        use tree = new LsmTree(testDataDir, 100)
        tree.Put("k1", "v1")
        tree.Flush()
        tree.Put("k2", "v2")

    use tree2 = new LsmTree(testDataDir)
    assertEqual (Some "v1") (tree2.Get "k1") "Should load from SSTable"
    assertEqual (Some "v2") (tree2.Get "k2") "Should load from WAL"

// ============================================================
// Data Structures (BloomFilter, SkipList)
// ============================================================

[<Fact>]
let ``BloomFilter_Empty_Behavior`` () =
    let bf = BloomFilter([||], 0)
    assertEqual true (bf.MightContain "any") "Empty BloomFilter true"

    let bf2 = BloomFilter.create 0
    assertEqual true (bf2.MightContain "any") "BloomFilter created with 0 size"

[<Fact>]
let ``BloomFilter_FalsePositiveRate`` () =
    let numEntries = 1000
    let bf = BloomFilter.create numEntries

    for i = 1 to numEntries do
        bf.Add(sprintf "key_%d" i)

    let numTests = 10000
    let mutable falsePositives = 0

    for i = 1 to numTests do
        let key = sprintf "miss_%d" i

        if bf.MightContain key then
            falsePositives <- falsePositives + 1

    let fpr = float falsePositives / float numTests
    Assert.True(fpr < 0.02, sprintf "False positive rate too high: %f" fpr)

[<Fact>]
let ``SkipList_Properties_Sorting_by_Keys`` () =
    let sl = SkipList()
    sl.Put("k3", 1L, "v3")
    sl.Put("k1", 2L, "v1")
    sl.Put("k2", 3L, "v2")
    let entries = sl.Entries()

    assertEqual
        [ "k1", 2L, Some "v1"; "k2", 3L, Some "v2"; "k3", 1L, Some "v3" ]
        entries
        "SkipList should maintain sorted order"

// ============================================================
// Concurrency & Stress Tests
// ============================================================

[<Fact>]
let ``Get_from_ImmutableMemTable_Race`` () =
    let testDataDir = getTestDir "imm_race"
    use tree = new LsmTree(testDataDir, 1000)
    tree.Put("race_k", "race_v")

    let tasks =
        [| for _ = 1 to 10 do
               yield
                   Task.Run(fun () ->
                       for _ = 1 to 10 do
                           tree.Flush()
                           tree.Get "race_k" |> ignore
                           tree.Put("race_k", "race_v")
                           tree.Get "race_k" |> ignore
                           tree.Delete "race_k"
                           tree.Get "race_k" |> ignore) |]

    Task.WaitAll tasks
    Assert.True(true, "Should not crash during concurrent flush/get")

[<Fact>]
let ``SkipList_Concurrency_Stress`` () =
    let list = SkipList()
    let numThreads = 20
    let numOps = 2000

    let tasks =
        [| for i = 1 to numThreads do
               yield
                   Task.Run(fun () ->
                       for j = 1 to numOps do
                           list.Put(sprintf "key%d" (j % 50), int64 (i * numOps + j), sprintf "val%d" j)

                           if j % 10 = 0 then
                               list.Find(sprintf "key%d" (j % 50), Int64.MaxValue) |> ignore) |]

    Task.WaitAll tasks
    let entries = list.Entries()
    Assert.True(entries.Length > 0)
