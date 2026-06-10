namespace LsmTree

type ITransaction =
    inherit System.IDisposable
    abstract member Put: key: string * value: string -> unit
    abstract member Delete: key: string -> unit
    abstract member Get: key: string -> string option
    abstract member Commit: unit -> unit
    abstract member Rollback: unit -> unit

type ILsmTree =
    abstract member Get: key: string * snapshot: int64 option -> string option
    abstract member CommitTransaction: ops: (string * string option) list -> unit
    abstract member ReleaseSnapshot: snapshot: int64 -> unit

type LsmTransaction(lsm: ILsmTree, snapshot: int64) =
    let mutable ops = []
    let mutable finished = false

    let checkFinished () =
        if finished then
            failwith "Transaction already finished."

    interface ITransaction with
        member _.Put(key, value) =
            checkFinished ()
            ops <- (key, Some value) :: ops

        member _.Delete key =
            checkFinished ()
            ops <- (key, None) :: ops

        member _.Get key =
            checkFinished ()
            let local = ops |> Seq.tryFind (fun (k, _) -> k = key)

            match local with
            | Some(_, Some v) -> Some v
            | Some(_, None) -> None
            | None -> lsm.Get(key, Some snapshot)

        member this.Commit() =
            checkFinished ()

            try
                lsm.CommitTransaction(ops |> Seq.rev |> Seq.toList)
            finally
                (this :> ITransaction).Dispose()

        member this.Rollback() =
            checkFinished ()

            try
                ops <- []
            finally
                (this :> ITransaction).Dispose()

        member _.Dispose() =
            if not finished then
                lsm.ReleaseSnapshot snapshot
                finished <- true
