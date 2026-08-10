module LsmTree.Benchmark

open System
open System.IO
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open LsmTree

[<AbstractClass>]
type BenchmarkBase(testDirName: string) =
    let testDir = Path.Combine(Environment.CurrentDirectory, testDirName)
    let mutable db: LsmTree = Unchecked.defaultof<_>

    member _.TestDir = testDir
    member _.Db = db

    member _.DoSetup() =
        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

        Directory.CreateDirectory testDir |> ignore
        db <- new LsmTree(testDir)

    member _.DoCleanup() =
        try
            db.Close()
        with _ ->
            ()

        if Directory.Exists testDir then
            try
                Directory.Delete(testDir, true)
            with _ ->
                ()

[<MemoryDiagnoser>]
type PutBenchmark() =
    inherit BenchmarkBase "bench_put"

    [<Params(10000)>]
    member val N = 0 with get, set

    [<Params(1, 100)>]
    member val ValueSize = 0 with get, set

    member val Value = "" with get, set

    [<IterationSetup>]
    member this.Setup() =
        base.DoSetup()
        this.Value <- String('x', this.ValueSize)

    [<IterationCleanup>]
    member _.Cleanup() = base.DoCleanup()

    [<Benchmark(Baseline = true)>]
    member this.SequentialPut() =
        let v = this.Value

        for i = 1 to this.N do
            base.Db.Put($"k{i}", v)

    [<Benchmark>]
    member this.ConcurrentPut() =
        let v = this.Value
        let db = base.Db
        Parallel.For(1, this.N + 1, fun i -> db.Put($"ck{i}", v)) |> ignore

    [<Benchmark>]
    member this.TransactionPut() =
        let v = this.Value
        use tx = base.Db.BeginTransaction()

        for i = 1 to this.N do
            tx.Put($"tk{i}", v)

        tx.Commit()

[<MemoryDiagnoser>]
type GetBenchmark() =
    inherit BenchmarkBase "bench_get"
    let rand = Random 42

    [<Params(10000, 30000)>]
    member val N = 0 with get, set

    [<GlobalSetup>]
    member this.Setup() =
        base.DoSetup()
        use tx = base.Db.BeginTransaction()

        for i = 1 to this.N do
            tx.Put($"k{i}", "v")

        tx.Commit()

    [<GlobalCleanup>]
    member _.Cleanup() = base.DoCleanup()

    [<Benchmark>]
    member this.RandomHitGet() =
        let target = $"k{rand.Next(1, this.N)}"
        base.Db.Get target |> ignore

    [<Benchmark>]
    member this.RandomMissGet() =
        let target = $"miss_{rand.Next(1, this.N)}"
        base.Db.Get target |> ignore

    [<Benchmark>]
    member this.SequentialGet() =
        for i = 1 to this.N do
            base.Db.Get $"k{i}" |> ignore

[<MemoryDiagnoser>]
type DeleteBenchmark() =
    inherit BenchmarkBase "bench_del"

    [<Params(10000)>]
    member val N = 0 with get, set

    [<IterationSetup>]
    member this.Setup() =
        base.DoSetup()
        use tx = base.Db.BeginTransaction()

        for i = 1 to this.N do
            tx.Put($"k{i}", "v")

        tx.Commit()

    [<IterationCleanup>]
    member _.Cleanup() = base.DoCleanup()

    [<Benchmark(Baseline = true)>]
    member this.SequentialDelete() =
        for i = 1 to this.N do
            base.Db.Delete $"k{i}"

    [<Benchmark>]
    member this.ConcurrentDelete() =
        let db = base.Db
        Parallel.For(1, this.N + 1, fun i -> db.Delete $"k{i}") |> ignore

    [<Benchmark>]
    member this.TransactionDelete() =
        use tx = base.Db.BeginTransaction()

        for i = 1 to this.N do
            tx.Delete $"k{i}"

        tx.Commit()

[<MemoryDiagnoser>]
type MixedWorkloadBenchmark() =
    inherit BenchmarkBase "bench_mixed"
    let rand = Random 42

    [<Params(10000)>]
    member val N = 0 with get, set

    [<IterationSetup>]
    member this.Setup() =
        base.DoSetup()
        use tx = base.Db.BeginTransaction()

        for i = 1 to this.N do
            tx.Put($"k{i}", "v")

        tx.Commit()

    [<IterationCleanup>]
    member _.Cleanup() = base.DoCleanup()

    [<Benchmark>]
    member this.ReadHeavy() =
        for _ = 1 to this.N do
            if rand.Next 100 < 90 then
                base.Db.Get $"k{rand.Next(1, this.N)}" |> ignore
            else
                base.Db.Put($"k{rand.Next(1, this.N)}", "v")

    [<Benchmark>]
    member this.WriteHeavy() =
        for _ = 1 to this.N do
            if rand.Next 100 < 50 then
                base.Db.Get $"k{rand.Next(1, this.N)}" |> ignore
            else
                base.Db.Put($"k{rand.Next(1, this.N)}", "v")

[<EntryPoint>]
let main argv =
    printfn "Starting LSM-Tree Benchmarks..."
    BenchmarkRunner.Run<PutBenchmark>() |> ignore
    BenchmarkRunner.Run<GetBenchmark>() |> ignore
    BenchmarkRunner.Run<DeleteBenchmark>() |> ignore
    BenchmarkRunner.Run<MixedWorkloadBenchmark>() |> ignore
    0
