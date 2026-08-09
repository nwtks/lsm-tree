module LsmTree.Tests.BloomFilterTests

open Xunit
open LsmTree

[<Fact>]
let ``BloomFilter keyIndex forces odd h2 so probes spread`` () =
    let bf = BloomFilter.create 100

    let keyIndexMethod =
        typeof<BloomFilter>
            .GetMethod(
                "keyIndex",
                System.Reflection.BindingFlags.NonPublic
                ||| System.Reflection.BindingFlags.Instance
            )

    Assert.NotNull keyIndexMethod

    let positions =
        [| for seed in 0..6 -> keyIndexMethod.Invoke(bf, [| box 0u; box 0u; box seed |]) :?> struct (int * int) |]

    assertEqual 7 (positions |> Array.distinct |> Array.length) "h2 = 0 must not collapse all probes"

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
let ``BloomFilter empty filter always returns true for MightContain`` () =
    let bf = BloomFilter([||], 0)
    assertEqual true (bf.MightContain "any") "Empty BloomFilter true"

    let bf2 = BloomFilter.create 0
    assertEqual true (bf2.MightContain "any") "BloomFilter created with 0 size"

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
let ``BloomFilter create with negative capacity uses default`` () =
    let bf = BloomFilter.create -1
    Assert.NotNull bf
    bf.Add "test_key"
    assertEqual true (bf.MightContain "test_key") "Added key should be found"
