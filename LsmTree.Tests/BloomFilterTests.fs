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

[<Fact>]
let ``BloomFilter Add and MightContain for specific keys`` () =
    let bf = BloomFilter.create 10
    bf.Add "apple"
    bf.Add "banana"
    bf.Add "cherry"

    assertEqual true (bf.MightContain "apple") "apple should be found"
    assertEqual true (bf.MightContain "banana") "banana should be found"
    assertEqual true (bf.MightContain "cherry") "cherry should be found"

[<Fact>]
let ``BloomFilter handles empty string key`` () =
    let bf = BloomFilter.create 10
    bf.Add ""
    assertEqual true (bf.MightContain "") "Empty string key should be found"

[<Fact>]
let ``BloomFilter duplicate adds do not corrupt filter`` () =
    let bf = BloomFilter.create 10
    bf.Add "dup"
    bf.Add "dup"
    bf.Add "dup"
    assertEqual true (bf.MightContain "dup") "Key added multiple times should still be found"

[<Fact>]
let ``BloomFilter create with negative capacity uses default`` () =
    let bf = BloomFilter.create -1
    Assert.NotNull bf
    bf.Add "test_key"
    assertEqual true (bf.MightContain "test_key") "Added key should be found"
