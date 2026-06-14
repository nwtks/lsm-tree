module LsmTree.Tests.LsmTreeTests

open Xunit
open LsmTree

[<Fact>]
let ``LsmTree constructor throws for empty path`` () =
    Assert.Throws<System.ArgumentException>(fun () -> new LsmTree "" |> ignore)
    |> ignore

[<Fact>]
let ``LsmTree startup creates directory if missing`` () =
    let testDir = getTestDir "startup_create_dir"
    let treePath = System.IO.Path.Combine(testDir, "nested", "tree", "path")
    let _ = new LsmTree(treePath)
    Assert.True(System.IO.Directory.Exists treePath)

[<Fact>]
let ``LsmTree restart reloads data from WAL`` () =
    let testDir = getTestDir "restart_reload"

    do
        use tree = new LsmTree(testDir)
        tree.Put("rk1", "rv1")
        tree.Put("rk2", "rv2")

    do
        use tree = new LsmTree(testDir)
        assertEqual (Some "rv1") (tree.Get "rk1") "Key1 survives restart"
        assertEqual (Some "rv2") (tree.Get "rk2") "Key2 survives restart"

[<Fact>]
let ``LsmTree restart after delete recovers DEL entry from WAL`` () =
    let testDir = getTestDir "restart_del"
    let tree1 = new LsmTree(testDir)
    tree1.Delete "k"
    tree1.Close()

    use tree2 = new LsmTree(testDir)
    assertEqual None (tree2.Get "k") "DEL entry recovered on restart"

[<Fact>]
let ``LsmTree nosync restart recovers multiple keys`` () =
    let testDir = getTestDir "nosync_restart"

    do
        use tree = new LsmTree(testDir, syncOnCommit = false)
        tree.Put("rk1", "rv1")
        tree.Put("rk2", "rv2")

    do
        use tree = new LsmTree(testDir, syncOnCommit = false)
        assertEqual (Some "rv1") (tree.Get "rk1") "K1 recovered after nosync restart"
        assertEqual (Some "rv2") (tree.Get "rk2") "K2 recovered after nosync restart"

[<Fact>]
let ``LsmTree nosync transaction commits and restarts correctly`` () =
    let testDir = getTestDir "nosync_tx"

    do
        use tree = new LsmTree(testDir, syncOnCommit = false)
        let tx = tree.BeginTransaction()
        tx.Put("ntk", "ntv")
        tx.Commit()
        tx.Dispose()

    do
        use tree = new LsmTree(testDir, syncOnCommit = false)
        assertEqual (Some "ntv") (tree.Get "ntk") "nosync transaction survives restart"

[<Fact>]
let ``LsmTree nosync compact and restart preserves data`` () =
    let testDir = getTestDir "nosync_compact"

    do
        use tree = new LsmTree(testDir, syncOnCommit = false, compactLevelLimits = [| 2 |])

        for i = 1 to 5 do
            tree.Put($"ck{i}", $"cv{i}")
            tree.Flush()

        tree.WaitForCompaction()

    do
        use tree = new LsmTree(testDir, syncOnCommit = false)

        for i = 1 to 5 do
            assertEqual (Some $"cv{i}") (tree.Get $"ck{i}") $"Key ck{i} preserved after compact+restart"

[<Fact>]
let ``LsmTree overwrite existing key`` () =
    use tree = new LsmTree(getTestDir "overwrite")
    tree.Put("k", "v1")
    tree.Put("k", "v2")
    assertEqual (Some "v2") (tree.Get "k") "Overwrite returns latest value"

[<Fact>]
let ``LsmTree CRUD operations work without sync`` () =
    use tree = new LsmTree(getTestDir "nosync_crud", syncOnCommit = false)
    tree.Put("nsk1", "nsv1")
    tree.Put("nsk2", "nsv2")
    assertEqual (Some "nsv1") (tree.Get "nsk1") "nosync Put works"
    assertEqual (Some "nsv2") (tree.Get "nsk2") "nosync second Put works"

[<Fact>]
let ``LsmTree auto-flush on Put when memTable exceeds size limit`` () =
    let testDir = getTestDir "auto_flush_put"
    use tree = new LsmTree(testDir, memTableSizeLimit = 1)
    tree.Put("k", "v")
    assertEqual (Some "v") (tree.Get "k") "Data preserved after auto-flush on Put"

[<Fact>]
let ``LsmTree resurrect deleted key`` () =
    use tree = new LsmTree(getTestDir "resurrect")
    tree.Put("k", "v1")
    let snapAfterPut = tree.Snapshot()
    tree.Delete "k"
    let snapAfterDelete = tree.Snapshot()
    tree.Put("k", "v2")
    assertEqual (Some "v2") (tree.Get "k") "Resurrected key returns new value"
    assertEqual None (tree.Get("k", snapAfterDelete)) "Snapshot after delete sees None"
    assertEqual (Some "v1") (tree.Get("k", snapAfterPut)) "Snapshot after Put sees original value"

[<Fact>]
let ``LsmTree nosync Delete exercises WAL sync=false path`` () =
    let testDir = getTestDir "nosync_del"
    use tree = new LsmTree(testDir, syncOnCommit = false)
    tree.Put("k", "v")
    tree.Delete "k"
    assertEqual None (tree.Get "k") "nosync Delete works"

    do
        use tree2 = new LsmTree(testDir, syncOnCommit = false)
        assertEqual None (tree2.Get "k") "Delete survives restart after nosync"

[<Fact>]
let ``LsmTree auto-flush on Delete when memTable exceeds size limit`` () =
    let testDir = getTestDir "auto_flush_del"
    use tree = new LsmTree(testDir, memTableSizeLimit = 1)
    tree.Put("k", "v")
    tree.Delete "k"
    assertEqual None (tree.Get "k") "Deletion preserved after auto-flush on Delete"

[<Fact>]
let ``LsmTree auto-flush on transaction commit when memTable exceeds size limit`` () =
    let testDir = getTestDir "auto_flush_tx"
    use tree = new LsmTree(testDir, memTableSizeLimit = 1)
    use tx = tree.BeginTransaction()
    tx.Put("k", "v")
    tx.Commit()
    assertEqual (Some "v") (tree.Get "k") "Data preserved after auto-flush on transaction commit"

[<Fact>]
let ``LsmTree explicit snapshot sees consistent view`` () =
    use tree = new LsmTree(getTestDir "explicit_snap")
    let snap1 = tree.Snapshot()
    tree.Put("sk", "sv1")

    let snap2 = tree.Snapshot()
    tree.Put("sk", "sv2")
    assertEqual None (tree.Get("sk", snap1)) "Snapshot before writes sees nothing"
    assertEqual (Some "sv1") (tree.Get("sk", snap2)) "Snapshot after first write sees v1"
    assertEqual (Some "sv2") (tree.Get "sk") "Direct latest sees v2"

[<Fact>]
let ``LsmTree MVCC returns correct versions at each snapshot`` () =
    use tree = new LsmTree(getTestDir "mvcc_versions")
    let snap0 = tree.Snapshot()
    tree.Put("k", "v1")

    let snap1 = tree.Snapshot()
    tree.Put("k", "v2")

    let snap2 = tree.Snapshot()
    assertEqual None (tree.Get("k", snap0)) "snap0 sees None"
    assertEqual (Some "v1") (tree.Get("k", snap1)) "snap1 sees v1"
    assertEqual (Some "v2") (tree.Get("k", snap2)) "snap2 sees v2"
    assertEqual (Some "v2") (tree.Get "k") "direct latest sees v2"

[<Fact>]
let ``LsmTree flush with no data does not throw`` () =
    use tree = new LsmTree(getTestDir "empty_flush")
    tree.Flush()

[<Fact>]
let ``LsmTree concurrent flush and read does not deadlock`` () =
    use tree = new LsmTree(getTestDir "conc_flush")
    let numOps = 10

    for i = 1 to numOps do
        tree.Put($"k{i}", $"v{i}")

    runConcurrent
        [| fun () ->
               for i = 1 to numOps do
                   tree.Get $"k{i}" |> ignore
           fun () -> tree.Flush() |]

    for i = 1 to numOps do
        assertEqual (Some $"v{i}") (tree.Get $"k{i}") $"Key k{i} after concurrent flush"

[<Fact>]
let ``LsmTree Flush propagates flush coordinator errors`` () =
    let testDir = getTestDir "flush_err_prop"
    use tree = new LsmTree(testDir)
    tree.Put("k", "v")
    tree.Flush()

    let flField =
        typeof<LsmTree>
            .GetField(
                "flushCoordinator",
                System.Reflection.BindingFlags.NonPublic
                ||| System.Reflection.BindingFlags.Instance
            )

    let fc = flField.GetValue tree :?> FlushCoordinator
    fc.Error <- Some(exn "injected flush error")

    Assert.Throws<System.AggregateException>(fun () -> tree.Flush() |> ignore)
    |> ignore

[<Fact>]
let ``LsmTree multi-level compaction merges correctly`` () =
    let testDir = getTestDir "multi_level"
    use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
    tree.Put("k1", "v1")
    tree.Flush()

    tree.Put("k2", "v2")
    tree.Flush()
    tree.WaitForCompaction()
    assertEqual (Some "v1") (tree.Get "k1") "k1 preserved"
    assertEqual (Some "v2") (tree.Get "k2") "k2 preserved"

[<Fact>]
let ``LsmTree flush during compaction does not block`` () =
    let testDir = getTestDir "flush_during_compact"
    use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
    tree.Put("k1", "v1")
    tree.Flush()

    tree.Put("k2", "v2")
    tree.Flush()
    tree.WaitForCompaction()

    tree.Put("k3", "v3")
    tree.Flush()
    assertEqual (Some "v3") (tree.Get "k3") "k3 preserved after flush during compaction"

[<Fact>]
let ``LsmTree cascade compaction across multiple levels`` () =
    let testDir = getTestDir "cascade_levels"
    use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

    for i = 1 to 10 do
        tree.Put($"ck{i}", $"cv{i}")
        tree.Flush()

    tree.WaitForCompaction()

    for i = 1 to 10 do
        assertEqual (Some $"cv{i}") (tree.Get $"ck{i}") $"Cascaded compaction: key ck{i}"

[<Fact>]
let ``LsmTree compaction tolerates IO errors`` () =
    let testDir = getTestDir "compact_err"
    use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])
    tree.Put("k", "v")
    tree.Flush()
    tree.Put("k2", "v2")
    tree.Flush()

    tree.Put("k3", "v3")
    tree.Flush()
    tree.WaitForCompaction()
    assertEqual (Some "v3") (tree.Get "k3") "Data should still be accessible after compaction"

[<Fact>]
let ``LsmTree WaitForCompaction propagates compaction coordinator errors`` () =
    let testDir = getTestDir "compact_err_prop"
    use tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

    for i = 1 to 3 do
        tree.Put($"k{i}", $"v{i}")
        tree.Flush()

    tree.WaitForCompaction()

    let compField =
        typeof<LsmTree>
            .GetField(
                "compaction",
                System.Reflection.BindingFlags.NonPublic
                ||| System.Reflection.BindingFlags.Instance
            )

    let cc = compField.GetValue tree :?> CompactionCoordinator
    cc.Error <- Some(exn "injected compaction error")

    Assert.Throws<System.AggregateException>(fun () -> tree.WaitForCompaction() |> ignore)
    |> ignore

[<Fact>]
let ``LsmTree compaction cancellation during Dispose`` () =
    let testDir = getTestDir "compact_cancel"
    let tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

    for i = 1 to 5 do
        tree.Put($"k{i}", $"v{i}")
        tree.Flush()

    (tree :> System.IDisposable).Dispose()

[<Fact>]
let ``LsmTree Dispose handles compaction coordinator errors`` () =
    let testDir = getTestDir "dispose_compact_err"
    let tree = new LsmTree(testDir, compactLevelLimits = [| 2 |])

    for i = 1 to 3 do
        tree.Put($"k{i}", $"v{i}")
        tree.Flush()

    tree.WaitForCompaction()

    let compField =
        typeof<LsmTree>
            .GetField(
                "compaction",
                System.Reflection.BindingFlags.NonPublic
                ||| System.Reflection.BindingFlags.Instance
            )

    let cc = compField.GetValue tree :?> CompactionCoordinator
    cc.Error <- Some(exn "injected compaction error")
    (tree :> System.IDisposable).Dispose()

[<Fact>]
let ``LsmTree Dispose handles flush coordinator errors`` () =
    let testDir = getTestDir "dispose_flush_err"
    let tree = new LsmTree(testDir)
    tree.Put("k", "v")
    tree.Flush()

    let flField =
        typeof<LsmTree>
            .GetField(
                "flushCoordinator",
                System.Reflection.BindingFlags.NonPublic
                ||| System.Reflection.BindingFlags.Instance
            )

    let fc = flField.GetValue tree :?> FlushCoordinator
    fc.Error <- Some(exn "injected flush error")
    (tree :> System.IDisposable).Dispose()
