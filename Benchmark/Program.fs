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

    [<Params(10000)>]
    member val N = 0 with get, set

    [<Params(1, 100)>]
    member val ValueSize = 0 with get, set

    member val Value = "" with get, set

    [<IterationSetup>]
    member this.Setup() =
        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

        db <- new LsmTree(testDir)
        this.Value <- String('x', this.ValueSize)

    [<IterationCleanup>]
    member _.Cleanup() =
        db.Close()

        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

    [<Benchmark(Baseline = true)>]
    member this.SequentialPut() =
        let v = this.Value

        for i = 1 to this.N do
            db.Put($"k{i}", v)

    [<Benchmark>]
    member this.ConcurrentPut() =
        let v = this.Value
        Parallel.For(1, this.N + 1, fun i -> db.Put($"ck{i}", v)) |> ignore

    [<Benchmark>]
    member this.TransactionPut() =
        let v = this.Value
        use tx = db.BeginTransaction()

        for i = 1 to this.N do
            tx.Put($"tk{i}", v)

        tx.Commit()

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
        use tx = db.BeginTransaction()

        for i = 1 to this.N do
            tx.Put($"k{i}", "v")

        tx.Commit()

    [<GlobalCleanup>]
    member _.Cleanup() =
        db.Close()

        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

    [<Benchmark>]
    member this.RandomHitGet() =
        let target = $"k{rand.Next(1, this.N)}"
        db.Get target |> ignore

    [<Benchmark>]
    member this.RandomMissGet() =
        let target = $"miss_{rand.Next(1, this.N)}"
        db.Get target |> ignore

    [<Benchmark>]
    member this.SequentialGet() =
        for i = 1 to this.N do
            db.Get $"k{i}" |> ignore

[<MemoryDiagnoser>]
type DeleteBenchmark() =
    let testDir = Path.Combine(Environment.CurrentDirectory, "bench_del")
    let mutable db: LsmTree = Unchecked.defaultof<_>

    [<Params(10000)>]
    member val N = 0 with get, set

    [<IterationSetup>]
    member this.Setup() =
        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

        db <- new LsmTree(testDir)
        use tx = db.BeginTransaction()

        for i = 1 to this.N do
            tx.Put($"k{i}", "v")

        tx.Commit()

    [<IterationCleanup>]
    member _.Cleanup() =
        db.Close()

        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

    [<Benchmark(Baseline = true)>]
    member this.SequentialDelete() =
        for i = 1 to this.N do
            db.Delete $"k{i}"

    [<Benchmark>]
    member this.ConcurrentDelete() =
        Parallel.For(1, this.N + 1, fun i -> db.Delete $"k{i}") |> ignore

    [<Benchmark>]
    member this.TransactionDelete() =
        use tx = db.BeginTransaction()

        for i = 1 to this.N do
            tx.Delete $"k{i}"

        tx.Commit()

[<MemoryDiagnoser>]
type MixedWorkloadBenchmark() =
    let testDir = Path.Combine(Environment.CurrentDirectory, "bench_mixed")
    let mutable db: LsmTree = Unchecked.defaultof<_>
    let rand = Random 42

    [<Params(10000)>]
    member val N = 0 with get, set

    [<IterationSetup>]
    member this.Setup() =
        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

        db <- new LsmTree(testDir)
        use tx = db.BeginTransaction()

        for i = 1 to this.N do
            tx.Put($"k{i}", "v")

        tx.Commit()

    [<IterationCleanup>]
    member _.Cleanup() =
        db.Close()

        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

    [<Benchmark>]
    member this.ReadHeavy() =
        for i = 1 to this.N do
            if rand.Next 100 < 90 then
                db.Get $"k{rand.Next(1, this.N)}" |> ignore
            else
                db.Put($"k{rand.Next(1, this.N)}", "v")

    [<Benchmark>]
    member this.WriteHeavy() =
        for i = 1 to this.N do
            if rand.Next 100 < 50 then
                db.Get $"k{rand.Next(1, this.N)}" |> ignore
            else
                db.Put($"k{rand.Next(1, this.N)}", "v")

[<EntryPoint>]
let main argv =
    printfn "Starting LSM-Tree Benchmarks..."
    let summary1 = BenchmarkRunner.Run<PutBenchmark>()
    let summary2 = BenchmarkRunner.Run<GetBenchmark>()
    let summary3 = BenchmarkRunner.Run<DeleteBenchmark>()
    let summary4 = BenchmarkRunner.Run<MixedWorkloadBenchmark>()
    0
