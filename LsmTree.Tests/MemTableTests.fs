module LsmTree.Tests.MemTableTests

open Xunit
open LsmTree

[<Fact>]
let ``MemTable Put and Get returns inserted value`` () =
    let mt = MemTable()
    mt.Put("k", 1L, "v1")
    assertEqual (Some(Some "v1")) (mt.Get("k", System.Int64.MaxValue)) "Put then Get returns value"

[<Fact>]
let ``MemTable Get returns None for non-existent key`` () =
    let mt = MemTable()
    assertEqual None (mt.Get("nonexistent", System.Int64.MaxValue)) "Get missing key returns None"

[<Fact>]
let ``MemTable Get respects snapshot isolation`` () =
    let mt = MemTable()
    mt.Put("k", 10L, "v1")
    mt.Put("k", 20L, "v2")
    assertEqual None (mt.Get("k", 5L)) "Snapshot before all entries returns None"
    assertEqual (Some(Some "v1")) (mt.Get("k", 10L)) "Snapshot at seq 10 sees v1"
    assertEqual (Some(Some "v2")) (mt.Get("k", 20L)) "Snapshot at seq 20 sees v2"
    assertEqual (Some(Some "v2")) (mt.Get("k", System.Int64.MaxValue)) "Max snapshot sees latest"

[<Fact>]
let ``MemTable Delete creates tombstone`` () =
    let mt = MemTable()
    mt.Put("k", 1L, "v1")
    mt.Delete("k", 2L)
    assertEqual (Some None) (mt.Get("k", System.Int64.MaxValue)) "Deleted key returns tombstone"
    assertEqual (Some(Some "v1")) (mt.Get("k", 1L)) "Earlier snapshot still sees value"

[<Fact>]
let ``MemTable SizeBytes increases after Put`` () =
    let mt = MemTable()
    let size0 = mt.SizeBytes
    mt.Put("k", 1L, "v")
    Assert.True(mt.SizeBytes > size0, "SizeBytes should increase after Put")

[<Fact>]
let ``MemTable SizeBytes increases after Delete`` () =
    let mt = MemTable()
    let size0 = mt.SizeBytes
    mt.Delete("k", 1L)
    Assert.True(mt.SizeBytes > size0, "SizeBytes should increase after Delete")

[<Fact>]
let ``MemTable Entries returns all entries in sorted order`` () =
    let mt = MemTable()
    mt.Put("z", 3L, "z_val")
    mt.Put("a", 1L, "a_val")
    mt.Put("m", 2L, "m_val")
    let entries = mt.Entries

    assertEqual
        [ "a", 1L, Some "a_val"; "m", 2L, Some "m_val"; "z", 3L, Some "z_val" ]
        entries
        "Entries should be sorted by key"

[<Fact>]
let ``MemTable Entries includes tombstone entries`` () =
    let mt = MemTable()
    mt.Put("k", 1L, "v1")
    mt.Delete("k", 2L)
    let entries = mt.Entries
    assertEqual [ "k", 2L, None; "k", 1L, Some "v1" ] entries "Entries returns highest seq first"

[<Fact>]
let ``MemTable Entries on empty returns empty list`` () =
    let mt = MemTable()
    assertEqual [] mt.Entries "Empty MemTable returns []"

[<Fact>]
let ``MemTable overwrite returns latest value`` () =
    let mt = MemTable()
    mt.Put("k", 1L, "v1")
    mt.Put("k", 2L, "v2")
    assertEqual (Some(Some "v2")) (mt.Get("k", System.Int64.MaxValue)) "Overwritten key returns latest value"
