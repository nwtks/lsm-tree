module LsmTree.Tests.BloomFilterTests

open Xunit
open LsmTree

[<Fact>]
let ``BloomFilter_Empty_Behavior`` () =
    let bf = BloomFilter([||], 0)
    assertEqual true (bf.MightContain "any") "Empty BloomFilter true"

    let bf2 = BloomFilter.create 0
    assertEqual true (bf2.MightContain "any") "BloomFilter created with 0 size"

[<Fact>]
let ``BloomFilter_FalsePositiveRate`` () =
    let numEntries = 1000
    let bf = BloomFilter.create numEntries

    for i = 1 to numEntries do
        bf.Add(sprintf "key_%d" i)

    let numTests = 10000
    let mutable falsePositives = 0

    for i = 1 to numTests do
        let key = sprintf "miss_%d" i

        if bf.MightContain key then
            falsePositives <- falsePositives + 1

    let fpr = float falsePositives / float numTests
    Assert.True(fpr < 0.02, sprintf "False positive rate too high: %f" fpr)
