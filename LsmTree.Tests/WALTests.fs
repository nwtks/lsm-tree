module LsmTree.Tests.WALTests

open Xunit
open LsmTree

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
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")

    do
        use sw = new System.IO.StreamWriter(walPath)
        sw.WriteLine "BEGIN 1"
        let k1 = WALRecovery.utf8ToBase64 "k1"
        let v1 = WALRecovery.utf8ToBase64 "v1"
        sw.WriteLine(sprintf "PUT 1 %s %s" k1 v1)

    use tree = new LsmTree(testDataDir)
    assertEqual None (tree.Get "k1") "Should not recover k1 because transaction was not committed"

    do
        use sw2 = System.IO.File.AppendText walPath
        sw2.WriteLine "COMMIT 1"

    use tree2 = new LsmTree(testDataDir)
    assertEqual (Some "v1") (tree2.Get "k1") "Should recover k1 after COMMIT marker is present"

[<Fact>]
let ``WAL_Recovery_Uncommitted_Transaction`` () =
    let testDataDir = getTestDir "tx_wal_uncommitted"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")

    do
        use writer = new System.IO.StreamWriter(walPath)
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
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")
    let k = WALRecovery.utf8ToBase64 "k"
    let v = WALRecovery.utf8ToBase64 "v"
    System.IO.File.WriteAllLines(walPath, [ "UNKNOWN 1 some data"; "BEGIN 2"; sprintf "PUT 2 %s %s" k v; "COMMIT 2" ])

    use tree = new LsmTree(testDataDir)
    assertEqual (Some "v") (tree.Get "k") "Should recover valid transaction even if unknown entry present"

[<Fact>]
let ``WAL_Recovery_Orphaned_Ops`` () =
    let testDataDir = getTestDir "wal_edge_orphan"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")
    let k_orphan = WALRecovery.utf8ToBase64 "key_orphan"
    let v_orphan = WALRecovery.utf8ToBase64 "val_orphan"
    System.IO.File.WriteAllLines(walPath, [ sprintf "PUT 3 %s %s" k_orphan v_orphan ])

    use tree = new LsmTree(testDataDir)
    assertEqual (Some "val_orphan") (tree.Get "key_orphan") "Orphaned PUT without BEGIN should be recovered"

[<Fact>]
let ``WAL_Recovery_Orphaned_Commit`` () =
    let testDataDir = getTestDir "wal_edge_orphan_commit"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")
    System.IO.File.WriteAllLines(walPath, [ "COMMIT 4" ])

    use tree = new LsmTree(testDataDir)
    assertEqual None (tree.Get "non_existent") "Orphaned COMMIT with no matching BEGIN should not crash"

[<Fact>]
let ``WAL_Recovery_Ignores_Malformed_Lines`` () =
    let testDataDir = getTestDir "wal_malformed"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")
    // Empty lines, non-numeric seq, too-few fields, and unknown verbs should all be skipped
    let lines = [ ""; "invalid"; "PUT abc k v"; "UNKNOWN 1 k v" ]
    System.IO.File.WriteAllLines(walPath, lines)

    let ops = WALRecovery.recover walPath |> Seq.toList
    assertEqual [] ops "Should ignore invalid WAL entries"
