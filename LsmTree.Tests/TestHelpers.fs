namespace LsmTree.Tests

open Xunit

[<AutoOpen>]
module TestHelpers =
    let getTestDir name =
        let dir =
            System.IO.Path.Combine(System.Environment.CurrentDirectory, "test_data_" + name)

        if System.IO.Directory.Exists dir then
            System.IO.Directory.Delete(dir, true)

        System.IO.Directory.CreateDirectory dir |> ignore
        dir

    let assertEqual expected actual msg =
        Assert.True((expected = actual), sprintf "%s\n  Expected: %A\n  Actual: %A" msg expected actual)
