namespace LsmTree

type ICoordinatorError =
    abstract Error: exn option with get, set

module LockExtensions =
    let withReadLock (lock: System.Threading.ReaderWriterLockSlim) f =
        lock.EnterReadLock()

        try
            f ()
        finally
            lock.ExitReadLock()

    let withWriteLock (lock: System.Threading.ReaderWriterLockSlim) f =
        lock.EnterWriteLock()

        try
            f ()
        finally
            lock.ExitWriteLock()

    let disposeOf (d: System.IDisposable) =
        try
            d.Dispose()
        with _ ->
            ()

    let checkCoordError (coord: ICoordinatorError) lockObj context =
        lock lockObj (fun () ->
            match coord.Error with
            | Some ex ->
                coord.Error <- None
                raise (System.AggregateException(context, ex))
            | None -> ())

    let logCoordError (coord: ICoordinatorError) lockObj context =
        lock lockObj (fun () ->
            match coord.Error with
            | Some ex ->
                coord.Error <- None
                eprintfn $"[WARN] LsmTree: {context} error during dispose: {ex.Message}"
            | None -> ())
