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
