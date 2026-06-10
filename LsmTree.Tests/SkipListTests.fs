module LsmTree.Tests.SkipListTests

open Xunit
open LsmTree

[<Fact>]
let ``SkipList randomLevel is within valid bounds`` () =
    let levels = Array.init 10000 (fun _ -> SkipList.randomLevel ())

    let allInRange =
        levels |> Array.forall (fun lvl -> lvl >= 1 && lvl <= SkipList.MAX_LEVEL)

    Assert.True(allInRange, $"All levels must be between 1 and {SkipList.MAX_LEVEL}")
    let hasSomeHighLevel = levels |> Array.exists (fun lvl -> lvl >= 4)
    Assert.True(hasSomeHighLevel, "At least one level should be >= 4 in 10000 trials")

[<Fact>]
let ``SkipList returns None for non-existent key`` () =
    let sl = SkipList()
    sl.Put("k1", 1L, "v1")
    assertEqual None (sl.Find("nonexistent", System.Int64.MaxValue)) "Should return None for missing key"
    assertEqual None (sl.Find("k1", 0L)) "Should return None when snapshot precedes entry"

[<Fact>]
let ``SkipList Find respects snapshot isolation`` () =
    let sl = SkipList()
    sl.Put("k", 10L, "v1")
    sl.Put("k", 20L, "v2")
    sl.Put("k", 30L, "v3")
    assertEqual None (sl.Find("k", 5L)) "Snapshot before all entries returns None"
    assertEqual (Some(Some "v1")) (sl.Find("k", 10L)) "Snapshot at seq 10 sees v1"
    assertEqual (Some(Some "v2")) (sl.Find("k", 20L)) "Snapshot at seq 20 sees v2"
    assertEqual (Some(Some "v3")) (sl.Find("k", System.Int64.MaxValue)) "Max snapshot sees latest"

[<Fact>]
let ``SkipList returns Some None for key with tombstone value`` () =
    let sl = SkipList()
    sl.Put("k", 1L)
    assertEqual (Some None) (sl.Find("k", System.Int64.MaxValue)) "Tombstone should return Some None"

[<Fact>]
let ``SkipList maintains sorted order by key`` () =
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
let ``SkipList handles concurrent access without crashing`` () =
    let list = SkipList()
    let numThreads = 20
    let numOps = 2000

    let tasks =
        [| for i = 1 to numThreads do
               yield
                   System.Threading.Tasks.Task.Run(fun () ->
                       for j = 1 to numOps do
                           list.Put($"key{j % 50}", int64 (i * numOps + j), $"val{j}")

                           if j % 10 = 0 then
                               list.Find($"key{j % 50}", System.Int64.MaxValue) |> ignore) |]

    System.Threading.Tasks.Task.WaitAll tasks
    let entries = list.Entries()
    Assert.True(entries.Length > 0)

[<Fact>]
let ``SkipList handles extreme CAS contention on same key`` () =
    let list = SkipList()
    let numThreads = 8
    let numOps = 10000

    let tasks =
        [| for i = 1 to numThreads do
               yield
                   System.Threading.Tasks.Task.Run(fun () ->
                       for j = 1 to numOps do
                           list.Put("sameKey", int64 (i * numOps + j), $"val{j}")

                           if j % 100 = 0 then
                               list.Find("sameKey", System.Int64.MaxValue) |> ignore) |]

    System.Threading.Tasks.Task.WaitAll tasks
    let entries = list.Entries()
    Assert.True(entries.Length > 0)

    match list.Find("sameKey", System.Int64.MaxValue) with
    | Some _ -> Assert.True(true, "sameKey should have a value")
    | None -> Assert.True(false, "sameKey should have a value after concurrent puts")

[<Fact>]
let ``SkipList Entries on empty list returns empty`` () =
    let sl = SkipList()
    assertEqual [] (sl.Entries()) "Empty SkipList should return []"
