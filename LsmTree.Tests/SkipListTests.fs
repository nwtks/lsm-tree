module LsmTree.Tests.SkipListTests

open Xunit
open LsmTree

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
                           list.Put(sprintf "key%d" (j % 50), int64 (i * numOps + j), sprintf "val%d" j)

                           if j % 10 = 0 then
                               list.Find(sprintf "key%d" (j % 50), System.Int64.MaxValue) |> ignore) |]

    System.Threading.Tasks.Task.WaitAll tasks
    let entries = list.Entries()
    Assert.True(entries.Length > 0)
