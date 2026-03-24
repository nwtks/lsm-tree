module LsmTree.Tests

open System
open System.IO
open LsmTree
open Xunit

let getTestDir name =
    let dir = Path.Combine(Environment.CurrentDirectory, "test_data_" + name)

    if Directory.Exists dir then
        Directory.Delete(dir, true)

    dir

let assertEqual expected actual msg =
    Assert.True((expected = actual), sprintf "%s\n  Expected: %A\n  Actual: %A" msg expected actual)

[<Fact>]
let ``Put_and_Get_from_MemTable`` () =
    let testDataDir = getTestDir "1"
    let tree = LsmTree testDataDir
    tree.Put("k1", "v1")
    assertEqual (Some "v1") (tree.Get "k1") "k1 should be v1"
    assertEqual None (tree.Get "k2") "k2 should be None"

[<Fact>]
let ``Delete_a_key_Tombstone`` () =
    let testDataDir = getTestDir "2"
    let tree = LsmTree testDataDir
    tree.Put("k1", "v1")
    tree.Delete "k1"
    assertEqual None (tree.Get "k1") "k1 should be deleted (Tombstone applied)"

[<Fact>]
let ``Flush_to_SSTable_and_Read`` () =
    let testDataDir = getTestDir "3"
    let tree = LsmTree(testDataDir, 10)
    tree.Put("key1", "value1")
    tree.Put("key2", "value2")
    tree.Flush()
    assertEqual (Some "value1") (tree.Get "key1") "Should read key1 from flushed SSTable"
    assertEqual (Some "value2") (tree.Get "key2") "Should read key2 from flushed SSTable"

[<Fact>]
let ``Auto_recovery_from_WAL`` () =
    let testDataDir = getTestDir "4"
    let tree1 = LsmTree testDataDir
    tree1.Put("wal_key1", "wal_val1")
    tree1.Put("wal_key2", "wal_val2")
    tree1.Delete "wal_key1"

    let tree2 = LsmTree testDataDir
    assertEqual None (tree2.Get "wal_key1") "wal_key1 should be deleted after recovery"
    assertEqual (Some "wal_val2") (tree2.Get "wal_key2") "wal_key2 should be recovered from WAL log"

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

[<Fact>]
let ``Multi_Level_Compaction_L0_L1`` () =
    let testDataDir = getTestDir "6"
    let tree = LsmTree(testDataDir, 10)

    for i = 1 to 5 do
        tree.Put(sprintf "c_k%d" i, sprintf "c_v%d" i)
        tree.Flush()

    tree.WaitForCompaction()

    for i = 1 to 5 do
        assertEqual (Some(sprintf "c_v%d" i)) (tree.Get(sprintf "c_k%d" i)) "Compacted keys must be readable"

    let l1Files = Directory.GetFiles(testDataDir, "L1_*.sst")
    Assert.True(l1Files.Length = 1, sprintf "Expected 1 compacted L1 file, but found %d" l1Files.Length)

[<Fact>]
let ``MVCC_Multi_Version_Concurrency_Control`` () =
    let testDataDir = getTestDir "7"
    let tree = LsmTree testDataDir
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
    assertEqual (Some "version1") (tree.Get("mvcc_key", snap1)) "Post-flush: Snapshot 1 should read version 1"

[<Fact>]
let ``Transaction_Commit_Visibility`` () =
    let testDataDir = getTestDir "tx1"
    let tree = LsmTree testDataDir
    use tx = tree.BeginTransaction()
    tx.Put("tx_k1", "tx_v1")
    assertEqual None (tree.Get "tx_k1") "Should not see uncommitted write"

    tx.Commit()
    assertEqual (Some "tx_v1") (tree.Get "tx_k1") "Should see committed write"

[<Fact>]
let ``Transaction_Rollback_Visibility`` () =
    let testDataDir = getTestDir "tx2"
    let tree = LsmTree testDataDir
    use tx = tree.BeginTransaction()
    tx.Put("tx_k2", "tx_v2")
    tx.Rollback()
    assertEqual None (tree.Get "tx_k2") "Should not see rolled back write"

[<Fact>]
let ``Transaction_Read_Own_Writes`` () =
    let testDataDir = getTestDir "tx3"
    let tree = LsmTree testDataDir
    use tx = tree.BeginTransaction()
    tx.Put("tx_k3", "tx_v3")
    assertEqual (Some "tx_v3") (tx.Get "tx_k3") "Should see its own uncommitted write"

    tx.Delete "tx_k3"
    assertEqual None (tx.Get "tx_k3") "Should see its own delete"

[<Fact>]
let ``Transaction_Snapshot_Isolation`` () =
    let testDataDir = getTestDir "tx4"
    let tree = LsmTree testDataDir
    tree.Put("k", "v1")
    use tx = tree.BeginTransaction()
    tree.Put("k", "v2")
    assertEqual (Some "v1") (tx.Get "k") "Transaction should see the snapshot at its start"

    tx.Commit()
    assertEqual (Some "v2") (tree.Get "k") "Final value should be v2"

[<Fact>]
let ``Transaction_Single_Sequence_Commit`` () =
    let testDataDir = getTestDir "tx5"
    let tree = LsmTree testDataDir
    use tx = tree.BeginTransaction()
    tx.Put("k1", "v1")
    tx.Put("k2", "v2")
    tx.Commit()

    let tree2 = LsmTree testDataDir
    let snap = tree2.Snapshot()
    assertEqual (Some "v1") (tree2.Get "k1") "k1 should be v1"
    assertEqual (Some "v2") (tree2.Get "k2") "k2 should be v2"
    assertEqual 1L snap "Both writes should share sequence 1"

[<Fact>]
let ``WAL_Atomic_Recovery`` () =
    let testDataDir = getTestDir "tx_wal_atomicity"

    if not (Directory.Exists testDataDir) then
        Directory.CreateDirectory testDataDir |> ignore

    let walPath = Path.Combine(testDataDir, "wal.log")
    let k1 = WALRecovery.utf8ToBase64 "k1"
    let v1 = WALRecovery.utf8ToBase64 "v1"
    use sw = new StreamWriter(walPath)
    sw.WriteLine "BEGIN 1"
    sw.WriteLine(sprintf "PUT 1 %s %s" k1 v1)
    sw.Close()

    let tree = LsmTree testDataDir
    assertEqual None (tree.Get "k1") "Should not recover k1 because transaction was not committed"

    use sw2 = File.AppendText walPath
    sw2.WriteLine "COMMIT 1"
    sw2.Close()

    let tree2 = LsmTree testDataDir
    assertEqual (Some "v1") (tree2.Get "k1") "Should recover k1 after COMMIT marker is present"

[<Fact>]
let ``Transaction_Isolation_Across_Flush`` () =
    let testDataDir = getTestDir "tx_flush"
    let tree = LsmTree(testDataDir, 1024)
    tree.Put("k1", "initial")
    use tx = tree.BeginTransaction()
    tree.Put("k1", "updated")
    tree.Flush()
    assertEqual (Some "initial") (tx.Get "k1") "Transaction must see its snapshot even after background flush"
    tx.Commit()

[<Fact>]
let ``Overwrite_Key_Multiple_Times`` () =
    let testDataDir = getTestDir "overwrite"
    let tree = LsmTree testDataDir
    tree.Put("k", "v1")
    tree.Put("k", "v2")
    tree.Put("k", "v3")
    assertEqual (Some "v3") (tree.Get "k") "Should see the latest version"

    tree.Flush()
    assertEqual (Some "v3") (tree.Get "k") "Should see the latest version after flush"

[<Fact>]
let ``Delete_NonExistent_Key`` () =
    let testDataDir = getTestDir "del_none"
    let tree = LsmTree testDataDir
    tree.Delete "no_such_key"
    assertEqual None (tree.Get "no_such_key") "Deleting non-existent key should be a no-op/tombstone but result in None"

[<Fact>]
let ``SSTable_Level_Parsing_and_Recovery_Ordering`` () =
    let testDataDir = getTestDir "sst_levels"

    if not (Directory.Exists testDataDir) then
        Directory.CreateDirectory testDataDir |> ignore

    let l1Path = Path.Combine(testDataDir, "L1_data.sst")
    let l0Path = Path.Combine(testDataDir, "L0_data.sst")
    let legacyPath = Path.Combine(testDataDir, "legacy.sst")
    SSTableWriter.write l1Path [ "k1", 1L, Some "v1_L1" ]
    SSTableWriter.write l0Path [ "k1", 200L, Some "v1_L0" ]
    SSTableWriter.write legacyPath [ "k9", 10L, Some "v9" ]

    let tree = LsmTree testDataDir

    assertEqual
        (Some "v1_L0")
        (tree.Get("k1", 300L))
        "Should prefer L0 over L1 (using high snapshot for manual recovery)"

    assertEqual (Some "v9") (tree.Get("k9", 100L)) "legacy.sst should be at level 0"

[<Fact>]
let ``WAL_Edge_Cases_Corruption_and_Orphans`` () =
    let testDataDir = getTestDir "wal_edge"

    if not (Directory.Exists testDataDir) then
        Directory.CreateDirectory testDataDir |> ignore

    let walPath = Path.Combine(testDataDir, "wal.log")
    let k = WALRecovery.utf8ToBase64 "k"
    let v = WALRecovery.utf8ToBase64 "v"
    File.WriteAllLines(walPath, [ "UNKNOWN 1 some data"; "BEGIN 2"; sprintf "PUT 2 %s %s" k v; "COMMIT 2" ])
    let tree1 = LsmTree testDataDir
    assertEqual (Some "v") (tree1.Get "k") "Should recover valid transaction even if unknown entry present"

    let k_orphan = WALRecovery.utf8ToBase64 "key_orphan"
    let v_orphan = WALRecovery.utf8ToBase64 "val_orphan"
    File.AppendAllLines(walPath, [ sprintf "PUT 3 %s %s" k_orphan v_orphan ])
    let tree2 = LsmTree testDataDir
    assertEqual (Some "val_orphan") (tree2.Get "key_orphan") "Orphaned Ops recovered"

    File.AppendAllLines(walPath, [ "COMMIT 4" ])
    let tree3 = LsmTree testDataDir
    assertEqual None (tree3.Get "non_existent") "Should not crash on orphaned commit"

[<Fact>]
let ``Transaction_Already_Finished_Errors`` () =
    let testDataDir = getTestDir "tx_errors"
    let tree = LsmTree testDataDir
    use tx = tree.BeginTransaction()
    tx.Commit()

    Assert.Throws<Exception>(fun () -> tx.Put("k", "v") |> ignore) |> ignore
    Assert.Throws<Exception>(fun () -> tx.Delete "k" |> ignore) |> ignore
    Assert.Throws<Exception>(fun () -> tx.Commit() |> ignore) |> ignore
    Assert.Throws<Exception>(fun () -> tx.Rollback() |> ignore) |> ignore

[<Fact>]
let ``BloomFilter_Empty_Behavior`` () =
    let bf = BloomFilter([||], 0)
    assertEqual true (bf.MightContain "any") "Empty BloomFilter true"

    let bf2 = BloomFilter.create 0
    assertEqual true (bf2.MightContain "any") "BloomFilter created with 0 size"

[<Fact>]
let ``SSTable_Load_Short_File_Handling`` () =
    let testDataDir = getTestDir "sst_short"

    if not (Directory.Exists testDataDir) then
        Directory.CreateDirectory testDataDir |> ignore

    let sstPath = Path.Combine(testDataDir, "L0_short.sst")
    File.WriteAllBytes(sstPath, [| 1uy; 2uy; 3uy |])
    let sst = SSTable sstPath
    assertEqual None (sst.Get("any", 0L)) "Should handle short/invalid SSTable file gracefully"

[<Fact>]
let ``WAL_Recover_NonExistent_File`` () =
    let ops = WALRecovery.recover "/tmp/non_existent_wal_path_xyz" |> Seq.toList
    assertEqual [] ops "Recovering non-existent file path"

[<Fact>]
let ``Test_MergeSSTables_Coverage`` () =
    let testDataDir = getTestDir "merge_cov"
    let limits = [| 1; 1 |]
    let tree = LsmTree(testDataDir, 1, limits)
    tree.Put("km", "v1")
    tree.Flush()
    tree.Put("km", "v2")
    tree.Flush()

    use tx = tree.BeginTransaction()
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
let ``Snapshot_Pruning_Verification`` () =
    let testDataDir = getTestDir "pruning"
    let tree = LsmTree(testDataDir, 10)
    tree.Put("kp", "v1")
    tree.Flush()
    tree.Put("kp", "v2")
    tree.Flush()
    tree.Put("kp", "v3")
    tree.Flush()

    use tx = tree.BeginTransaction()
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
let ``Get_from_ImmutableMemTable_Race`` () =
    let testDataDir = getTestDir "imm_race"
    let tree = LsmTree(testDataDir, 1000)
    tree.Put("race_k", "race_v")

    for i = 1 to 100 do
        System.Threading.Tasks.Task.Run(fun () -> tree.Flush()) |> ignore
        tree.Get "race_k" |> ignore
        tree.Put("race_k", "race_v")
        tree.Get "race_k" |> ignore
        tree.Delete "race_k"

    Assert.True(true, "Should not crash during concurrent flush/get")
