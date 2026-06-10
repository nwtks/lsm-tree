module LsmTree.Tests.TransactionTests

open Xunit
open LsmTree

[<Fact>]
let ``Transaction reads its own uncommitted writes`` () =
    let testDataDir = getTestDir "tx3"
    use tree = new LsmTree(testDataDir)
    use tx = tree.BeginTransaction()
    tx.Put("tx_k3", "tx_v3")
    assertEqual (Some "tx_v3") (tx.Get "tx_k3") "Should see its own uncommitted write"

    tx.Delete "tx_k3"
    assertEqual None (tx.Get "tx_k3") "Should see its own delete"

[<Fact>]
let ``Transaction delete-then-put within same transaction shadows correctly`` () =
    let testDataDir = getTestDir "iso_delete_put"
    use tree = new LsmTree(testDataDir)
    tree.Put("k", "original")

    use tx = tree.BeginTransaction()
    tx.Delete "k"
    assertEqual None (tx.Get "k") "Tx sees k as deleted after local delete"

    tx.Put("k", "new_value")
    assertEqual (Some "new_value") (tx.Get "k") "Tx sees new value after delete-then-put"

    tx.Commit()
    assertEqual (Some "new_value") (tree.Get "k") "Final: k = new_value"

[<Fact>]
let ``Transaction put-then-delete within same transaction shadows correctly`` () =
    let testDataDir = getTestDir "iso_put_delete"
    use tree = new LsmTree(testDataDir)

    use tx = tree.BeginTransaction()
    tx.Put("k", "temp")
    assertEqual (Some "temp") (tx.Get "k") "Tx sees local put"

    tx.Delete "k"
    assertEqual None (tx.Get "k") "Tx sees k as deleted after local delete (overrides put)"

    tx.Commit()
    assertEqual None (tree.Get "k") "k is deleted after commit"

[<Fact>]
let ``Transaction sees snapshot at start time`` () =
    let testDataDir = getTestDir "tx4"
    use tree = new LsmTree(testDataDir)
    tree.Put("k", "v1")
    use tx = tree.BeginTransaction()
    tree.Put("k", "v2")
    assertEqual (Some "v1") (tx.Get "k") "Transaction should see the snapshot at its start"

    tx.Commit()
    assertEqual (Some "v2") (tree.Get "k") "Final value should be v2"

[<Fact>]
let ``Transaction isolation persists across flushes`` () =
    let testDataDir = getTestDir "tx_flush"
    use tree = new LsmTree(testDataDir, 1024)
    tree.Put("k1", "initial")
    use tx = tree.BeginTransaction()
    tree.Put("k1", "updated")
    tree.Flush()
    assertEqual (Some "initial") (tx.Get "k1") "Transaction must see its snapshot even after background flush"

[<Fact>]
let ``Repeatable read — multiple reads of same key return consistent value`` () =
    let testDataDir = getTestDir "iso_repeat"
    use tree = new LsmTree(testDataDir)
    tree.Put("k", "original")

    use tx = tree.BeginTransaction()
    tree.Put("k", "modified")
    tree.Put("k", "modified2")

    for _ = 1 to 10 do
        assertEqual (Some "original") (tx.Get "k") "Repeatable read must return snapshot value"

    tx.Commit()
    assertEqual (Some "modified2") (tree.Get "k") "Latest value visible after commit"

[<Fact>]
let ``Repeatable read — multiple keys return consistent snapshot view`` () =
    let testDataDir = getTestDir "iso_multi"
    use tree = new LsmTree(testDataDir)
    tree.Put("a", "a1")
    tree.Put("b", "b1")

    use tx = tree.BeginTransaction()
    tree.Put("a", "a2")
    tree.Put("c", "c_new")
    assertEqual (Some "a1") (tx.Get "a") "Should see a1 from snapshot"
    assertEqual (Some "b1") (tx.Get "b") "Should see b1 from snapshot"
    assertEqual None (tx.Get "c") "Should NOT see c (inserted after snapshot)"

    tx.Commit()
    assertEqual (Some "a2") (tree.Get "a") "Latest a visible after commit"
    assertEqual (Some "c_new") (tree.Get "c") "c visible after commit"

[<Fact>]
let ``Snapshot isolation survives compaction — old versions preserved for snapshots`` () =
    let testDataDir = getTestDir "iso_compact"
    use tree = new LsmTree(testDataDir, 100)
    tree.Put("k", "v1")
    tree.Flush()

    use tx = tree.BeginTransaction()
    assertEqual (Some "v1") (tx.Get "k") "Tx sees v1 at start"

    tree.Put("k", "v2")
    tree.Flush()
    tree.Put("k", "v3")
    tree.Flush()
    tree.WaitForCompaction()
    assertEqual (Some "v1") (tx.Get "k") "Tx reads v1 through compaction"

    tx.Commit()
    assertEqual (Some "v3") (tree.Get "k") "Current view is v3 after tx commit"

[<Fact>]
let ``Active snapshot prevents compaction from pruning visible versions`` () =
    let testDataDir = getTestDir "iso_snap_prune"
    let limits = [| 1 |]
    use tree = new LsmTree(testDataDir, 500, compactLevelLimits = limits)
    tree.Put("k", "v1")
    tree.Flush()

    use tx = tree.BeginTransaction()
    assertEqual (Some "v1") (tx.Get "k") "Tx sees v1"

    for i = 2 to 10 do
        tree.Put("k", $"v{i}")
        tree.Flush()
        tree.WaitForCompaction()

    assertEqual (Some "v1") (tx.Get "k") "Tx reads v1 even after many compactions"

    tx.Commit()

    use tx2 = tree.BeginTransaction()
    assertEqual (Some "v10") (tx2.Get "k") "New transaction reads latest after snapshot released"

[<Fact>]
let ``Transaction isolation works across MemTable, immutable, and SSTable`` () =
    let testDataDir = getTestDir "iso_layers"
    use tree = new LsmTree(testDataDir, 1024)
    tree.Put("k1", "mem_v1")

    let snap = tree.Snapshot()
    tree.Put("k1", "sst_v2")
    tree.Flush()
    tree.Put("k1", "mem_v3")
    assertEqual (Some "mem_v1") (tree.Get("k1", snap)) "Snapshot sees version from before flush"

    let snap2 = tree.Snapshot()
    assertEqual (Some "mem_v3") (tree.Get("k1", snap2)) "Latest snapshot sees current MemTable"

[<Fact>]
let ``Transaction commit makes writes visible to others`` () =
    let testDataDir = getTestDir "tx1"
    use tree = new LsmTree(testDataDir)
    use tx = tree.BeginTransaction()
    tx.Put("tx_k1", "tx_v1")
    assertEqual None (tree.Get "tx_k1") "Should not see uncommitted write"

    tx.Commit()
    assertEqual (Some "tx_v1") (tree.Get "tx_k1") "Should see committed write"

[<Fact>]
let ``Transaction single-sequence commit shares sequence number`` () =
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
let ``Transaction empty commit works safely`` () =
    let testDataDir = getTestDir "tx_empty"
    use tree = new LsmTree(testDataDir)
    use tx = tree.BeginTransaction()
    tx.Commit()
    assertEqual None (tree.Get "any") "Empty commit should work safely"

[<Fact>]
let ``Concurrent transactions to different keys succeed independently`` () =
    let testDataDir = getTestDir "iso_diff_keys"
    use tree = new LsmTree(testDataDir)
    use tx1 = tree.BeginTransaction()
    use tx2 = tree.BeginTransaction()
    tx1.Put("key_a", "from_tx1")
    tx2.Put("key_b", "from_tx2")
    assertEqual (Some "from_tx1") (tx1.Get "key_a") "Tx1 sees its own write"
    assertEqual (Some "from_tx2") (tx2.Get "key_b") "Tx2 sees its own write"
    assertEqual None (tx1.Get "key_b") "Tx1 should not see Tx2's uncommitted write"
    assertEqual None (tx2.Get "key_a") "Tx2 should not see Tx1's uncommitted write"

    tx1.Commit()
    tx2.Commit()
    assertEqual (Some "from_tx1") (tree.Get "key_a") "key_a visible after tx1 commit"
    assertEqual (Some "from_tx2") (tree.Get "key_b") "key_b visible after tx2 commit"

[<Fact>]
let ``Concurrent transactions to same key — snapshot isolation preserved`` () =
    let testDataDir = getTestDir "iso_same_key"
    use tree = new LsmTree(testDataDir)
    tree.Put("conflict", "initial")
    use tx1 = tree.BeginTransaction()
    use tx2 = tree.BeginTransaction()
    tx2.Put("conflict", "from_tx2")
    tx2.Commit()
    assertEqual (Some "from_tx2") (tree.Get "conflict") "Tx2's write is visible after commit"

    assertEqual
        (Some "initial")
        (tx1.Get "conflict")
        "Tx1 should still see snapshot value despite Tx2's concurrent commit"

    tx1.Put("conflict", "from_tx1")
    tx1.Commit()
    assertEqual (Some "from_tx1") (tree.Get "conflict") "Tx1 overwrites Tx2 (last writer wins)"

[<Fact>]
let ``Multiple concurrent transactions with overlapping lifetimes`` () =
    let testDataDir = getTestDir "iso_overlap"
    use tree = new LsmTree(testDataDir)
    tree.Put("x", "x0")
    tree.Put("y", "y0")

    use tx1 = tree.BeginTransaction()
    assertEqual (Some "x0") (tx1.Get "x") "Tx1 sees x0"

    use tx2 = tree.BeginTransaction()
    tx2.Put("x", "x_from_tx2")
    tx2.Commit()

    use tx3 = tree.BeginTransaction()
    assertEqual (Some "x_from_tx2") (tx3.Get "x") "Tx3 sees Tx2's commit"
    assertEqual (Some "y0") (tx3.Get "y") "Tx3 sees y0"
    assertEqual (Some "x0") (tx1.Get "x") "Tx1 snapshot isolated from Tx2"
    assertEqual (Some "y0") (tx1.Get "y") "Tx1 sees y0"

    tx1.Put("y", "y_from_tx1")
    tx1.Commit()
    assertEqual (Some "x_from_tx2") (tx3.Get "x") "Tx3 still read Tx2's value"

    tx3.Put("x", "x_from_tx3")
    tx3.Commit()

    assertEqual (Some "x_from_tx3") (tree.Get "x") "Last writer on x wins"
    assertEqual (Some "y_from_tx1") (tree.Get "y") "Last writer on y wins"

[<Fact>]
let ``Transaction rollback restores original values`` () =
    let testDataDir = getTestDir "tx_rollback"
    use tree = new LsmTree(testDataDir)
    tree.Put("k1", "v1")
    use tx = tree.BeginTransaction()
    tx.Put("k1", "v2")
    assertEqual (Some "v2") (tx.Get "k1") "Transaction should see local write"

    tx.Rollback()
    assertEqual (Some "v1") (tree.Get "k1") "Database should remain v1 after rollback"

[<Fact>]
let ``Rollback releases snapshot — no stale snapshot retention`` () =
    let testDataDir = getTestDir "iso_rollback_snap"
    use tree = new LsmTree(testDataDir)
    tree.Put("k", "v1")

    let snapBefore = tree.Snapshot()
    let tx1 = tree.BeginTransaction()
    let _ = tree.Snapshot()
    tx1.Rollback()
    (tx1 :> System.IDisposable).Dispose()

    tree.Put("k", "v2")
    tree.Flush()
    assertEqual (Some "v1") (tree.Get("k", snapBefore)) "snapBefore should still read v1"

    tree.Put("clean", "ok")
    assertEqual (Some "ok") (tree.Get "clean") "Engine is healthy after rollback"

[<Fact>]
let ``Transaction double dispose does not throw`` () =
    let testDataDir = getTestDir "tx_double_dispose"
    use tree = new LsmTree(testDataDir)
    let tx = tree.BeginTransaction()
    (tx :> System.IDisposable).Dispose()

    (tx :> System.IDisposable).Dispose()
    Assert.True(true, "Should not throw")

[<Fact>]
let ``Transaction operations on finished transaction throw`` () =
    let testDataDir = getTestDir "tx_errors"
    use tree = new LsmTree(testDataDir)
    use tx = tree.BeginTransaction()
    tx.Commit()

    Assert.Throws<System.Exception>(fun () -> tx.Put("k", "v") |> ignore) |> ignore
    Assert.Throws<System.Exception>(fun () -> tx.Delete "k" |> ignore) |> ignore
    Assert.Throws<System.Exception>(fun () -> tx.Commit() |> ignore) |> ignore
    Assert.Throws<System.Exception>(fun () -> tx.Rollback() |> ignore) |> ignore
