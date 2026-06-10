module LsmTree.Tests.WALTests

open Xunit
open LsmTree

[<Fact>]
let ``WALRecovery base64ToUtf8 throws on invalid input`` () =
    Assert.Throws<System.FormatException>(fun () -> WALRecovery.base64ToUtf8 "!!!invalid-base64!!!" |> ignore)

[<Fact>]
let ``WAL recovery handles invalid base64 gracefully`` () =
    let testDataDir = getTestDir "wal_bad_base64"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")
    let k = WALRecovery.utf8ToBase64 "good_key"
    let v = WALRecovery.utf8ToBase64 "good_val"

    System.IO.File.WriteAllLines(
        walPath,
        [| "PUT 1 !!!invalid-base64!!! !!!data!!!"
           "BEGIN 2"
           $"PUT 2 {k} {v}"
           "COMMIT 2" |]
    )

    use tree = new LsmTree(testDataDir)
    assertEqual (Some "good_val") (tree.Get "good_key") "Should recover valid transaction despite malformed entry"

[<Fact>]
let ``WAL recovery handles non-existent file gracefully`` () =
    let testDataDir = getTestDir "wal_no_exist"

    let ops =
        WALRecovery.recover (System.IO.Path.Combine(testDataDir, "nonexistent.wal"))
        |> Seq.toList

    assertEqual [] ops "Recovering non-existent file path"

[<Fact>]
let ``WAL recovery with empty file returns empty`` () =
    let testDataDir = getTestDir "wal_empty"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")
    System.IO.File.WriteAllText(walPath, "")

    let ops = WALRecovery.recover walPath |> Seq.toList
    assertEqual [] ops "Empty WAL file should produce no entries"

[<Fact>]
let ``WAL recovery ignores malformed lines`` () =
    let testDataDir = getTestDir "wal_malformed"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")

    let lines = [ ""; "invalid"; "PUT abc k v"; "UNKNOWN 1 k v" ]
    System.IO.File.WriteAllLines(walPath, lines)

    let ops = WALRecovery.recover walPath |> Seq.toList
    assertEqual [] ops "Should ignore invalid WAL entries"

[<Fact>]
let ``WAL recovery handles orphaned PUT lines`` () =
    let testDataDir = getTestDir "wal_edge_orphan"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")
    let k_orphan = WALRecovery.utf8ToBase64 "key_orphan"
    let v_orphan = WALRecovery.utf8ToBase64 "val_orphan"
    System.IO.File.WriteAllLines(walPath, [ $"PUT 3 {k_orphan} {v_orphan}" ])

    use tree = new LsmTree(testDataDir)
    assertEqual (Some "val_orphan") (tree.Get "key_orphan") "Orphaned PUT without BEGIN should be recovered"

[<Fact>]
let ``WAL recovery handles orphaned COMMIT lines`` () =
    let testDataDir = getTestDir "wal_edge_orphan_commit"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")
    System.IO.File.WriteAllLines(walPath, [ "COMMIT 4" ])

    use tree = new LsmTree(testDataDir)
    assertEqual None (tree.Get "non_existent") "Orphaned COMMIT with no matching BEGIN should not crash"

[<Fact>]
let ``WAL recovery ignores unknown entries`` () =
    let testDataDir = getTestDir "wal_edge"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")
    let k = WALRecovery.utf8ToBase64 "k"
    let v = WALRecovery.utf8ToBase64 "v"
    System.IO.File.WriteAllLines(walPath, [ "UNKNOWN 1 some data"; "BEGIN 2"; $"PUT 2 {k} {v}"; "COMMIT 2" ])

    use tree = new LsmTree(testDataDir)
    assertEqual (Some "v") (tree.Get "k") "Should recover valid transaction even if unknown entry present"

[<Fact>]
let ``WAL recovery handles DEL entries`` () =
    let testDataDir = getTestDir "wal_del_recovery"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")
    let k = WALRecovery.utf8ToBase64 "del_key"
    let k2 = WALRecovery.utf8ToBase64 "keep_key"
    let v2 = WALRecovery.utf8ToBase64 "keep_val"
    System.IO.File.WriteAllLines(walPath, [ $"DEL 1 {k}"; $"PUT 2 {k2} {v2}" ])

    use tree = new LsmTree(testDataDir)
    assertEqual None (tree.Get "del_key") "DEL key should be absent after recovery"
    assertEqual (Some "keep_val") (tree.Get "keep_key") "PUT key should be present after recovery"

[<Fact>]
let ``WAL atomic recovery skips uncommitted transactions`` () =
    let testDataDir = getTestDir "tx_wal_atomicity"
    let walPath = System.IO.Path.Combine(testDataDir, "wal.log")

    do
        use sw = new System.IO.StreamWriter(walPath)
        sw.WriteLine "BEGIN 1"
        let k1 = WALRecovery.utf8ToBase64 "k1"
        let v1 = WALRecovery.utf8ToBase64 "v1"
        sw.WriteLine $"PUT 1 {k1} {v1}"

    use tree = new LsmTree(testDataDir)
    assertEqual None (tree.Get "k1") "Should not recover k1 because transaction was not committed"

    do
        use sw2 = System.IO.File.AppendText walPath
        sw2.WriteLine "COMMIT 1"

    use tree2 = new LsmTree(testDataDir)
    assertEqual (Some "v1") (tree2.Get "k1") "Should recover k1 after COMMIT marker is present"


[<Fact>]
let ``WAL recovery restores data after restart`` () =
    let testDataDir = getTestDir "4"
    use tree1 = new LsmTree(testDataDir)
    tree1.Put("wal_key1", "wal_val1")
    tree1.Put("wal_key2", "wal_val2")
    tree1.Delete "wal_key1"

    use tree2 = new LsmTree(testDataDir)
    assertEqual None (tree2.Get "wal_key1") "wal_key1 should be deleted after recovery"
    assertEqual (Some "wal_val2") (tree2.Get "wal_key2") "wal_key2 should be recovered from WAL log"

[<Fact>]
let ``WAL dispose handles I/O errors gracefully`` () =
    let testDataDir = getTestDir "wal_io_error"
    let walPath = System.IO.Path.Combine(testDataDir, "wal_io.log")
    let wal = new WAL(walPath)

    let streamField =
        typeof<WAL>
            .GetField(
                "stream",
                System.Reflection.BindingFlags.NonPublic
                ||| System.Reflection.BindingFlags.Instance
            )

    let stream = streamField.GetValue(wal) :?> System.IO.FileStream
    stream.Close()

    (wal :> System.IDisposable).Dispose()
    Assert.True(true, "WAL Dispose should not throw when underlying stream is closed")

[<Fact>]
let ``WAL double dispose does not throw`` () =
    let testDataDir = getTestDir "wal_double_dispose"
    let walPath = System.IO.Path.Combine(testDataDir, "wal_dd.log")

    let wal = new WAL(walPath)
    (wal :> System.IDisposable).Dispose()
    (wal :> System.IDisposable).Dispose()
    Assert.True(true, "WAL double dispose should not throw")
