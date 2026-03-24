module LsmTree.Benchmark

open System
open System.IO
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open LsmTree

[<MemoryDiagnoser>]
type PutBenchmark() =
    let testDir = Path.Combine(Environment.CurrentDirectory, "bench_put")
    let mutable db: LsmTree = Unchecked.defaultof<_>

    [<Params(10000, 30000)>]
    member val N = 0 with get, set

    [<IterationSetup>]
    member _.Setup() =
        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

        db <- new LsmTree(testDir)

    [<IterationCleanup>]
    member _.Cleanup() =
        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

    [<Benchmark(Baseline = true)>]
    member this.SequentialPut() =
        for i = 1 to this.N do
            db.Put(sprintf "k%d" i, "v")

    [<Benchmark>]
    member this.ConcurrentPut() =
        Parallel.For(1, this.N + 1, fun i -> db.Put(sprintf "ck%d" i, "v")) |> ignore

[<MemoryDiagnoser>]
type GetBenchmark() =
    let testDir = Path.Combine(Environment.CurrentDirectory, "bench_get")
    let mutable db: LsmTree = Unchecked.defaultof<_>
    let rand = Random 42

    [<Params(10000, 30000)>]
    member val N = 0 with get, set

    [<GlobalSetup>]
    member this.Setup() =
        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

        db <- new LsmTree(testDir)

        for i = 1 to this.N do
            db.Put(sprintf "k%d" i, "v")

        db.Flush()

    [<GlobalCleanup>]
    member _.Cleanup() =
        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

    [<Benchmark>]
    member this.RandomHitGet() =
        let target = sprintf "k%d" (rand.Next(1, this.N))
        db.Get target |> ignore

    [<Benchmark>]
    member this.RandomMissGet() =
        let target = sprintf "miss_%d" (rand.Next(1, this.N))
        db.Get target |> ignore

[<EntryPoint>]
let main argv =
    printfn "Starting LSM-Tree Benchmarks..."
    let summary1 = BenchmarkRunner.Run<PutBenchmark>()
    let summary2 = BenchmarkRunner.Run<GetBenchmark>()
    0
