namespace LsmTree

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
