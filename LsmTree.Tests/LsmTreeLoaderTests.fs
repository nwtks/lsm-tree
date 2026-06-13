module LsmTree.Tests.LsmTreeLoaderTests

open Xunit
open LsmTree

[<Fact>]
let ``LsmTreeLoader validateCompactLevelLimits throws for empty array`` () =
    Assert.Throws<System.ArgumentException>(fun () -> LsmTreeLoader.validateCompactLevelLimits [||] |> ignore)

[<Fact>]
let ``LsmTreeLoader validateCompactLevelLimits throws for negative values`` () =
    Assert.Throws<System.ArgumentException>(fun () ->
        LsmTreeLoader.validateCompactLevelLimits [| 4; -1; 100 |] |> ignore)
