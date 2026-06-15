namespace LsmTree.Tests

open Xunit

[<AutoOpen>]
module TestHelpers =
    let withTestDir name f =
        let dir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "test_data_" + name)

        if System.IO.Directory.Exists dir then
            System.IO.Directory.Delete(dir, true)

        System.IO.Directory.CreateDirectory dir |> ignore

        try
            f dir
        finally
            if System.IO.Directory.Exists dir then
                System.IO.Directory.Delete(dir, true)

    let assertEqual expected actual msg =
        Assert.True((expected = actual), $"{msg}\n  Expected: {expected}\n  Actual: {actual}")

    let runConcurrent tasks =
        tasks
        |> Array.map (fun f -> System.Action f |> System.Threading.Tasks.Task.Run)
        |> System.Threading.Tasks.Task.WaitAll
