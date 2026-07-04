module LsmTree.Tests.SkipListTests

open Xunit
open LsmTree

[<Fact>]
let ``SkipList randomLevel returns value between 1 and MAX_LEVEL`` () =
    let levels = Array.init 10000 (fun _ -> SkipList.randomLevel ())

    let allInRange =
        levels |> Array.forall (fun lvl -> lvl >= 1 && lvl <= SkipList.MAX_LEVEL)

    Assert.True(allInRange, $"All levels must be between 1 and {SkipList.MAX_LEVEL}")
    let hasSomeHighLevel = levels |> Array.exists (fun lvl -> lvl >= 4)
    Assert.True(hasSomeHighLevel, "At least one level should be >= 4 in 10000 trials")

[<Fact>]
let ``SkipList Find respects snapshot isolation`` () =
    let sl = SkipList()
    sl.Put("k", 10L, "v1")
    sl.Put("k", 20L, "v2")
    sl.Put("k", 30L, "v3")
    assertEqual NotFound (sl.Find("k", 5L)) "Snapshot before all entries returns NotFound"
    assertEqual (Found "v1") (sl.Find("k", 10L)) "Snapshot at seq 10 sees v1"
    assertEqual (Found "v2") (sl.Find("k", 20L)) "Snapshot at seq 20 sees v2"
    assertEqual (Found "v3") (sl.Find("k", System.Int64.MaxValue)) "Max snapshot sees latest"

[<Fact>]
let ``SkipList Find returns tombstone for deleted key`` () =
    let sl = SkipList()
    sl.Put("k", 1L)
    assertEqual Tombstone (sl.Find("k", System.Int64.MaxValue)) "Tombstone should return Tombstone"

[<Fact>]
let ``SkipList Find returns NotFound for non-existent key`` () =
    let sl = SkipList()
    sl.Put("k1", 1L, "v1")
    assertEqual NotFound (sl.Find("nonexistent", System.Int64.MaxValue)) "Should return NotFound for missing key"
    assertEqual NotFound (sl.Find("k1", 0L)) "Should return NotFound when snapshot precedes entry"

[<Fact>]
let ``SkipList concurrent Put and Find completes without deadlock`` () =
    let list = SkipList()
    let numThreads = 20
    let numOps = 2000

    runConcurrent (
        Array.init numThreads (fun idx ->
            let i = idx + 1

            fun () ->
                for j = 1 to numOps do
                    list.Put("sameKey", int64 (i * numOps + j), $"val{j}")

                    if j % 100 = 0 then
                        list.Find("sameKey", System.Int64.MaxValue) |> ignore)
    )

    let entries = list.Entries()
    Assert.True(entries.Length > 0)

[<Fact>]
let ``SkipList extreme CAS contention on same key succeeds`` () =
    let list = SkipList()
    let numThreads = 8
    let numOps = 10000

    runConcurrent (
        Array.init numThreads (fun idx ->
            let i = idx + 1

            fun () ->
                for j = 1 to numOps do
                    list.Put("sameKey", int64 (i * numOps + j), $"val{j}")

                    if j % 100 = 0 then
                        list.Find("sameKey", System.Int64.MaxValue) |> ignore)
    )

    let entries = list.Entries()
    Assert.True(entries.Length > 0)

    Assert.True(
        (match list.Find("sameKey", System.Int64.MaxValue) with
         | Found _ -> true
         | _ -> false),
        "sameKey should have a value after concurrent puts"
    )

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
let ``SkipList Entries returns empty list for empty SkipList`` () =
    let sl = SkipList()
    assertEqual [] (sl.Entries()) "Empty SkipList should return []"

[<Fact>]
let ``SkipList EntriesRange returns entries within range`` () =
    let sl = SkipList()
    sl.Put("a", 1L, "va")
    sl.Put("b", 2L, "vb")
    sl.Put("c", 3L, "vc")
    sl.Put("d", 4L, "vd")
    let range = sl.EntriesRange("b", "c")
    assertEqual 2 range.Length "Two entries in [b,c]"
    assertEqual ("b", 2L, Some "vb") range.[0] "First entry is b"
    assertEqual ("c", 3L, Some "vc") range.[1] "Second entry is c"

[<Fact>]
let ``SkipList EntriesRange includes same-key entries with descending seq`` () =
    let sl = SkipList()
    sl.Put("k", 10L, "v1")
    sl.Put("k", 20L, "v2")
    sl.Put("k", 30L, "v3")
    let range = sl.EntriesRange("k", "k")
    assertEqual 3 range.Length "Three entries for key k"
    assertEqual ("k", 30L, Some "v3") range.[0] "Highest seq first"
    assertEqual ("k", 20L, Some "v2") range.[1] "Middle seq"
    assertEqual ("k", 10L, Some "v1") range.[2] "Lowest seq last"

[<Fact>]
let ``SkipList EntriesRange includes tombstones in range`` () =
    let sl = SkipList()
    sl.Put("k", 1L, "v")
    sl.Put("k", 2L)
    let range = sl.EntriesRange("k", "k")
    assertEqual 2 range.Length "Tombstone and value in range"
    assertEqual ("k", 2L, None) range.[0] "Tombstone (seq=2, higher) first"
    assertEqual ("k", 1L, Some "v") range.[1] "Value (seq=1) second"

[<Fact>]
let ``SkipList EntriesRange returns empty when fromKey > toKey`` () =
    let sl = SkipList()
    sl.Put("a", 1L, "va")
    assertEqual [] (sl.EntriesRange("z", "a")) "fromKey>toKey should return []"
