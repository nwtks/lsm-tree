namespace LsmTree

type IIterator =
    inherit System.IDisposable
    abstract MoveNext: unit -> bool
    abstract Current: (string * string)

type SourceCursor(entries: (string * int64 * string option)[]) =
    member val Entries = entries
    member val Pos = 0 with get, set
    member this.IsDone = this.Pos >= this.Entries.Length

    member this.CurrentKey =
        let k, _, _ = entries.[this.Pos]
        k

    member this.CurrentEntry = entries.[this.Pos]

module RangeIteratorModule =
    [<TailCall>]
    let rec pickMinKey (cursors: SourceCursor[]) idx bestIdx =
        if idx >= cursors.Length then
            bestIdx
        else
            let nextIdx =
                if cursors.[idx].IsDone then
                    bestIdx
                else
                    match bestIdx with
                    | -1 -> idx
                    | b ->
                        let cmp =
                            System.String.CompareOrdinal(cursors.[idx].CurrentKey, cursors.[b].CurrentKey)

                        if cmp < 0 then idx else b

            pickMinKey cursors (idx + 1) nextIdx

    let drainKey (cursors: SourceCursor seq) key snapshot =
        let mutable versions = []

        for c in cursors do
            while not c.IsDone && c.CurrentKey = key do
                let _, seq, value = c.CurrentEntry
                versions <- (seq, value) :: versions
                c.Pos <- c.Pos + 1

        let mutable bestSeq = -1L
        let mutable bestValue: string option = None

        for seq, value in versions do
            if seq > bestSeq && seq <= snapshot then
                bestSeq <- seq
                bestValue <- value

        bestValue

    [<TailCall>]
    let rec moveNext cursors snapshot =
        match pickMinKey cursors 0 -1 with
        | -1 -> false, Unchecked.defaultof<string * string>
        | idx ->
            let key = cursors.[idx].CurrentKey

            match drainKey cursors key snapshot with
            | Some v -> true, (key, v)
            | None -> moveNext cursors snapshot

[<Sealed>]
type RangeIterator(snapshotManager: LsmTreeSnapshot, sources: (string * int64 * string option)[][], snapshot) =
    let cursors =
        sources |> Array.filter (fun e -> e.Length > 0) |> Array.map SourceCursor

    let mutable currentValue = Unchecked.defaultof<string * string>
    let mutable hasCurrent = false
    let mutable disposed = false

    member _.MoveNext() =
        let hasNext, value = RangeIteratorModule.moveNext cursors snapshot
        currentValue <- value
        hasCurrent <- hasNext
        hasCurrent

    member _.Current =
        if not hasCurrent then
            System.InvalidOperationException "Iterator has no current value" |> raise

        currentValue

    interface IIterator with
        member this.MoveNext() = this.MoveNext()
        member this.Current = this.Current

    interface System.IDisposable with
        member _.Dispose() =
            if not disposed then
                disposed <- true
                snapshotManager.ReleaseSnapshot snapshot
