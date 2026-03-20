module LsmTree.Tests

open System
open System.IO
open LsmTree
open Xunit

let getTestDir name =
    let dir = Path.Combine(Environment.CurrentDirectory, "test_data_" + name)

    if Directory.Exists(dir) then
        Directory.Delete(dir, true)

    dir

let assertEqual expected actual msg =
    Assert.True((expected = actual), sprintf "%s\n  Expected: %A\n  Actual: %A" msg expected actual)

[<Fact>]
let ``Put_and_Get_from_MemTable`` () =
    let testDataDir = getTestDir "1"
    let tree = new LsmTree(testDataDir)
    tree.Put("k1", "v1")
    assertEqual (Some "v1") (tree.Get("k1")) "k1 should be v1"
    assertEqual None (tree.Get("k2")) "k2 should be None"

[<Fact>]
let ``Delete_a_key_Tombstone`` () =
    let testDataDir = getTestDir "2"
    let tree = new LsmTree(testDataDir)
    tree.Put("k1", "v1")
    tree.Delete("k1")
    assertEqual None (tree.Get("k1")) "k1 should be deleted (Tombstone applied)"

[<Fact>]
let ``Flush_to_SSTable_and_Read`` () =
    let testDataDir = getTestDir "3"
    let tree = new LsmTree(testDataDir, memTableSizeLimit = 10)
    tree.Put("key1", "value1")
    tree.Put("key2", "value2")
    tree.Flush() // Force flush to SSTable

    assertEqual (Some "value1") (tree.Get("key1")) "Should read key1 from flushed SSTable"
    assertEqual (Some "value2") (tree.Get("key2")) "Should read key2 from flushed SSTable"

[<Fact>]
let ``Auto_recovery_from_WAL`` () =
    let testDataDir = getTestDir "4"
    let tree1 = new LsmTree(testDataDir)
    tree1.Put("wal_key1", "wal_val1")
    tree1.Put("wal_key2", "wal_val2")
    tree1.Delete("wal_key1")

    let tree2 = new LsmTree(testDataDir)
    assertEqual None (tree2.Get("wal_key1")) "wal_key1 should be deleted after recovery"
    assertEqual (Some "wal_val2") (tree2.Get("wal_key2")) "wal_key2 should be recovered from WAL log"

[<Fact>]
let ``SkipList_Properties_Sorting_by_Keys`` () =
    let sl = new SkipList()
    sl.Put("k3", 1L, Some "v3")
    sl.Put("k1", 2L, Some "v1")
    sl.Put("k2", 3L, Some "v2")

    let entries = sl.Entries()

    assertEqual
        [ ("k1", 2L, Some "v1"); ("k2", 3L, Some "v2"); ("k3", 1L, Some "v3") ]
        entries
        "SkipList should maintain sorted order"

[<Fact>]
let ``Multi_Level_Compaction_L0_L1`` () =
    let testDataDir = getTestDir "6"
    let tree = new LsmTree(testDataDir, memTableSizeLimit = 10)

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
    let tree = new LsmTree(testDataDir)

    tree.Put("mvcc_key", "version1")
    let snap1 = tree.Snapshot()

    tree.Put("mvcc_key", "version2")
    let snap2 = tree.Snapshot()

    tree.Put("mvcc_key", "version3")
    let snap3 = tree.Snapshot()

    tree.Delete("mvcc_key")
    let snap4 = tree.Snapshot()

    assertEqual None (tree.Get("mvcc_key")) "Current timeline should have key deleted"

    assertEqual (Some "version3") (tree.Get("mvcc_key", snapshot = snap3)) "Snapshot 3 should read version 3"
    assertEqual (Some "version2") (tree.Get("mvcc_key", snapshot = snap2)) "Snapshot 2 should read version 2"
    assertEqual (Some "version1") (tree.Get("mvcc_key", snapshot = snap1)) "Snapshot 1 should read version 1"

    tree.Flush()
    tree.WaitForCompaction()

    assertEqual None (tree.Get("mvcc_key")) "Post-flush: Current timeline should have key deleted"

    assertEqual
        (Some "version3")
        (tree.Get("mvcc_key", snapshot = snap3))
        "Post-flush: Snapshot 3 should read version 3"

    assertEqual
        (Some "version1")
        (tree.Get("mvcc_key", snapshot = snap1))
        "Post-flush: Snapshot 1 should read version 1"
