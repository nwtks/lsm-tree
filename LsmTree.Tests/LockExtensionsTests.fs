module LsmTree.Tests.LockExtensionsTests

open Xunit
open LsmTree

[<Fact>]
let ``LockExtensions disposeOf swallows IOException from Dispose`` () =
    let target =
        { new System.IDisposable with
            member _.Dispose() = raise (System.IO.IOException "boom") }

    LockExtensions.disposeOf target

[<Fact>]
let ``LockExtensions disposeOf swallows ObjectDisposedException`` () =
    let target =
        { new System.IDisposable with
            member _.Dispose() =
                raise (System.ObjectDisposedException "boom") }

    LockExtensions.disposeOf target

[<Fact>]
let ``LockExtensions disposeOf swallows unexpected exceptions`` () =
    let target =
        { new System.IDisposable with
            member _.Dispose() =
                raise (System.InvalidOperationException "boom") }

    LockExtensions.disposeOf target

[<Fact>]
let ``LockExtensions disposeOf disposes cleanly`` () =
    let mutable disposed = false

    let target =
        { new System.IDisposable with
            member _.Dispose() = disposed <- true }

    LockExtensions.disposeOf target
    Assert.True(disposed, "Dispose was called")
