module LsmTree.Tests.TransactionTests

open Xunit
open LsmTree

[<Fact>]
let ``Transaction read own writes within transaction`` () =
    withTestDir "tx_read_own" (fun dir ->
        use tree = new LsmTree(dir)
        use tx = tree.BeginTransaction()
        tx.Put("a", "1")
        assertEqual (Some "1") (tx.Get "a") "Read own Put within same transaction"
        tx.Put("a", "2")
        assertEqual (Some "2") (tx.Get "a") "Read own overwrite within same transaction"
        tx.Delete "a"
        assertEqual None (tx.Get "a") "Read own Delete within same transaction"
        tx.Commit())

[<Fact>]
let ``Transaction Commit makes writes visible to new transactions`` () =
    withTestDir "tx_commit_vis" (fun dir ->
        use tree = new LsmTree(dir)
        let tx1 = tree.BeginTransaction()
        tx1.Put("ck", "cv")
        tx1.Commit()
        tx1.Dispose()

        use tx2 = tree.BeginTransaction()
        assertEqual (Some "cv") (tx2.Get "ck") "Committed write visible in new transaction"
        tx2.Commit())

[<Fact>]
let ``Transaction commit advances sequence number exactly once`` () =
    withTestDir "tx_single_seq" (fun dir ->
        use tree = new LsmTree(dir)
        let snapBefore = tree.Snapshot()

        use tx = tree.BeginTransaction()
        tx.Put("k", "v")
        tx.Commit()

        let snapAfter = tree.Snapshot()
        Assert.True(snapAfter > snapBefore))

[<Fact>]
let ``Transaction empty commit does not advance sequence`` () =
    withTestDir "tx_empty_commit" (fun dir ->
        use tree = new LsmTree(dir)
        let snapBefore = tree.Snapshot()

        use tx = tree.BeginTransaction()
        tx.Commit()

        let snapAfter = tree.Snapshot()
        assertEqual snapBefore snapAfter "Sequence should not advance on empty commit")

[<Fact>]
let ``Transaction delete after put shadows previous value`` () =
    withTestDir "tx_del_after_put" (fun dir ->
        use tree = new LsmTree(dir)
        use tx = tree.BeginTransaction()
        tx.Put("k", "v1")
        tx.Put("k", "v2")
        tx.Delete "k"
        assertEqual None (tx.Get "k") "Delete after put within same txn returns None"
        tx.Commit()

        use tx2 = tree.BeginTransaction()
        assertEqual None (tx2.Get "k") "Delete after put committed is visible to new txn"
        tx2.Commit())

[<Fact>]
let ``Transaction put after delete resurrects key`` () =
    withTestDir "tx_put_after_del" (fun dir ->
        use tree = new LsmTree(dir)
        use tx = tree.BeginTransaction()
        tx.Delete "k"
        tx.Put("k", "resurrected")
        assertEqual (Some "resurrected") (tx.Get "k") "Put after delete resurrects key within same txn"
        tx.Commit()

        use tx2 = tree.BeginTransaction()
        assertEqual (Some "resurrected") (tx2.Get "k") "Resurrected key visible to new txn"
        tx2.Commit())

[<Fact>]
let ``Transaction snapshot at start sees only committed data`` () =
    withTestDir "tx_snap_start" (fun dir ->
        use tree = new LsmTree(dir)
        let snap = tree.Snapshot()

        use tx = tree.BeginTransaction()
        tx.Put("sk", "sv")
        tx.Commit()
        assertEqual None (tree.Get("sk", snap)) "Snapshot created before txn sees uncommitted data"
        assertEqual (Some "sv") (tree.Get("sk", tree.Snapshot())) "Snapshot created after txn sees committed data")

[<Fact>]
let ``Transaction isolation across flushes`` () =
    withTestDir "tx_flush_iso" (fun dir ->
        use tree = new LsmTree(dir)
        let snap = tree.Snapshot()

        use tx = tree.BeginTransaction()
        tx.Put("fk", "fv")
        tx.Commit()
        tree.Flush()
        assertEqual None (tree.Get("fk", snap)) "Snapshot before flush should not see committed data"
        assertEqual (Some "fv") (tree.Get("fk", tree.Snapshot())) "New snapshot after flush sees data")

[<Fact>]
let ``Transaction repeatable read within same transaction`` () =
    withTestDir "tx_repeatable" (fun dir ->
        use tree = new LsmTree(dir)

        use tx = tree.BeginTransaction()
        tree.Put("rk", "rv")
        assertEqual None (tx.Get "rk") "Transaction sees snapshot-consistent view"
        tx.Commit())

[<Fact>]
let ``Transaction repeatable read consistent across multiple keys`` () =
    withTestDir "tx_repeat_multi" (fun dir ->
        use tree = new LsmTree(dir)
        use tx = tree.BeginTransaction()

        let snap = tree.Snapshot()
        tx.Put("k1", "v1")
        tx.Put("k2", "v2")
        tx.Commit()
        assertEqual None (tree.Get("k1", snap)) "Snapshot sees none for k1"
        assertEqual None (tree.Get("k2", snap)) "Snapshot sees none for k2")

[<Fact>]
let ``Transaction isolation survives compaction`` () =
    withTestDir "tx_compact_iso" (fun dir ->
        use tree = new LsmTree(dir)
        let snap = tree.Snapshot()

        let tx1 = tree.BeginTransaction()
        tx1.Put("ck", "cv")
        tx1.Commit()
        tx1.Dispose()
        tree.Flush()

        let tx2 = tree.BeginTransaction()
        tx2.Put("dk", "dv")
        tx2.Commit()
        tx2.Dispose()
        tree.Flush()
        tree.WaitForCompaction()
        assertEqual None (tree.Get("ck", snap)) "Old snapshot sees none after compaction"
        assertEqual None (tree.Get("dk", snap)) "Old snapshot sees none after compaction")

[<Fact>]
let ``Transaction active snapshot prevents pruning of MVCC data`` () =
    withTestDir "tx_active_snap" (fun dir ->
        use tree = new LsmTree(dir)
        let snap = tree.Snapshot()

        let tx1 = tree.BeginTransaction()
        tx1.Put("pk", "pv")
        tx1.Commit()
        tx1.Dispose()
        tree.Flush()
        tree.WaitForCompaction()
        assertEqual None (tree.Get("pk", snap)) "Old snapshot should still see nothing"
        assertEqual (Some "pv") (tree.Get("pk", tree.Snapshot())) "New snapshot sees data")

[<Fact>]
let ``Transaction isolation across multiple layers with flush`` () =
    withTestDir "tx_layer_iso" (fun dir ->
        use tree = new LsmTree(dir)
        let snap = tree.Snapshot()

        let tx1 = tree.BeginTransaction()
        tx1.Put("lk", "lv1")
        tx1.Commit()
        tx1.Dispose()
        tree.Flush()

        let tx2 = tree.BeginTransaction()
        tx2.Put("lk", "lv2")
        tx2.Commit()
        tx2.Dispose()
        assertEqual None (tree.Get("lk", snap)) "Old snapshot sees none after multiple ops")

[<Fact>]
let ``Transaction concurrent transactions on different keys`` () =
    withTestDir "tx_conc_diff" (fun dir ->
        use tree = new LsmTree(dir)
        let tx1 = tree.BeginTransaction()
        let tx2 = tree.BeginTransaction()
        tx1.Put("ka", "va")
        tx2.Put("kb", "vb")
        tx1.Commit()
        tx2.Commit()
        tx1.Dispose()
        tx2.Dispose()

        use tx3 = tree.BeginTransaction()
        assertEqual (Some "va") (tx3.Get "ka") "tx1's write visible"
        assertEqual (Some "vb") (tx3.Get "kb") "tx2's write visible"
        tx3.Commit())

[<Fact>]
let ``Transaction concurrent transactions on same key`` () =
    withTestDir "tx_conc_same" (fun dir ->
        use tree = new LsmTree(dir)
        let tx1 = tree.BeginTransaction()
        let tx2 = tree.BeginTransaction()
        tx1.Put("sk", "from_tx1")
        tx2.Put("sk", "from_tx2")
        tx1.Commit()
        tx2.Commit()
        tx1.Dispose()
        tx2.Dispose()

        use tx3 = tree.BeginTransaction()
        let actual = tx3.Get "sk"

        Assert.True(
            actual = Some "from_tx1" || actual = Some "from_tx2",
            $"Expected either tx1 or tx2 value, got {actual}"
        )

        tx3.Commit())

[<Fact>]
let ``Transaction multiple overlapping transactions`` () =
    withTestDir "tx_multi_overlap" (fun dir ->
        use tree = new LsmTree(dir)
        let snap = tree.Snapshot()

        let tx1 = tree.BeginTransaction()
        tx1.Put("mk", "mv1")
        tx1.Commit()
        tx1.Dispose()

        let tx2 = tree.BeginTransaction()
        tx2.Put("mk", "mv2")
        tx2.Commit()
        tx2.Dispose()

        use tx3 = tree.BeginTransaction()
        assertEqual (Some "mv2") (tx3.Get "mk") "Latest committed value visible"
        assertEqual None (tree.Get("mk", snap)) "Old snapshot sees nothing"
        tx3.Commit())

[<Fact>]
let ``Transaction Rollback discards uncommitted writes`` () =
    withTestDir "tx_rollback" (fun dir ->
        use tree = new LsmTree(dir)

        use tx = tree.BeginTransaction()
        tx.Put("rk", "rv")
        tx.Delete "rk2"
        tx.Rollback()
        assertEqual None (tree.Get("rk", tree.Snapshot())) "Rollback discards Put"
        assertEqual None (tree.Get("rk2", tree.Snapshot())) "Rollback discards Delete")

[<Fact>]
let ``Transaction Rollback releases snapshot sequence`` () =
    withTestDir "tx_rollback_snap" (fun dir ->
        use tree = new LsmTree(dir)
        let snapBefore = tree.Snapshot()

        use tx = tree.BeginTransaction()
        tx.Put("rs", "rv")
        tx.Rollback()

        let snapAfter = tree.Snapshot()
        assertEqual snapBefore snapAfter "Rollback should revert snapshot sequence")

[<Fact>]
let ``Transaction double dispose does not throw`` () =
    withTestDir "tx_double_dispose" (fun dir ->
        use tree = new LsmTree(dir)
        let tx = tree.BeginTransaction()
        tx.Dispose()
        tx.Dispose())

[<Fact>]
let ``Transaction finished transaction throws`` () =
    withTestDir "tx_finished_throws" (fun dir ->
        use tree = new LsmTree(dir)
        let tx = tree.BeginTransaction()
        tx.Commit()

        Assert.Throws<System.InvalidOperationException>(fun () -> tx.Put("k", "v") |> ignore)
        |> ignore

        Assert.Throws<System.InvalidOperationException>(fun () -> tx.Delete "k" |> ignore)
        |> ignore

        Assert.Throws<System.InvalidOperationException>(fun () -> tx.Get "k" |> ignore)
        |> ignore

        Assert.Throws<System.InvalidOperationException>(fun () -> tx.Commit() |> ignore)
        |> ignore

        Assert.Throws<System.InvalidOperationException>(fun () -> tx.Rollback() |> ignore)
        |> ignore)
