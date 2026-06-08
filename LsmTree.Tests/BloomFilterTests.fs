module LsmTree.Tests.BloomFilterTests

open Xunit
open LsmTree

[<Fact>]
let ``BloomFilter empty behavior always returns true`` () =
    let bf = BloomFilter([||], 0)
    assertEqual true (bf.MightContain "any") "Empty BloomFilter true"

    let bf2 = BloomFilter.create 0
    assertEqual true (bf2.MightContain "any") "BloomFilter created with 0 size"

[<Fact>]
let ``BloomFilter false positive rate is below 2 percent`` () =
    let numEntries = 1000
    let bf = BloomFilter.create numEntries

    for i = 1 to numEntries do
        bf.Add $"key_{i}"

    let numTests = 10000
    let mutable falsePositives = 0

    for i = 1 to numTests do
        let key = $"miss_{i}"

        if bf.MightContain key then
            falsePositives <- falsePositives + 1

    let fpr = float falsePositives / float numTests
    Assert.True(fpr < 0.02, $"False positive rate too high: {fpr}")
