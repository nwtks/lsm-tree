namespace LsmTree

type ITransaction =
    inherit System.IDisposable
    abstract member Put: key: string * value: string -> unit
    abstract member Delete: key: string -> unit
    abstract member Get: key: string -> string option
    abstract member Commit: unit -> unit
    abstract member Rollback: unit -> unit

type ILsmTree =
    abstract member Get: key: string * snapshot: SnapshotHandle -> string option
    abstract member CommitTransaction: ops: (string * string option) list -> unit
    abstract member RollbackTransaction: unit -> unit

type LsmTransaction(lsm: ILsmTree, snapshot: SnapshotHandle) =
    let ops = System.Collections.Generic.Dictionary<string, string option>()
    let mutable finished = false

    let checkFinished () =
        if finished then
            invalidOp "Transaction already finished."

    interface ITransaction with
        member _.Put(key, value) =
            checkFinished ()
            ops[key] <- Some value

        member _.Delete key =
            checkFinished ()
            ops[key] <- None

        member _.Get key =
            checkFinished ()

            match ops.TryGetValue key with
            | true, v -> v
            | false, _ -> lsm.Get(key, snapshot)

        member this.Commit() =
            checkFinished ()

            try
                ops |> Seq.map (|KeyValue|) |> Seq.toList |> lsm.CommitTransaction
            finally
                (this :> ITransaction).Dispose()

        member this.Rollback() =
            checkFinished ()

            try
                lsm.RollbackTransaction()
                ops.Clear()
            finally
                (this :> ITransaction).Dispose()

        member _.Dispose() =
            if not finished then
                snapshot.Dispose()
                finished <- true
