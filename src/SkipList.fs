namespace LsmTree

open System
open System.Threading

[<AllowNullLiteral>]
type SkipListNode(key: string, seq: int64, value: string option, level: int) =
    let next = Array.zeroCreate<SkipListNode> level
    member val Key = key
    member val Seq = seq
    member val Value = value with get, set
    member val Next = next

type SkipList() =
    [<Literal>]
    let MAX_LEVEL = 16

    [<Literal>]
    let P = 0.5

    let rand = Random()
    let head = SkipListNode("", Int64.MaxValue, None, MAX_LEVEL)
    let mutable currentLevel = 1

    let getCurrentLevel () = Volatile.Read(&currentLevel)

    let randomLevel () =
        let mutable lvl = 1

        while rand.NextDouble() < P && lvl < MAX_LEVEL do
            lvl <- lvl + 1

        lvl

    let next (next: SkipListNode) key seq =
        not (isNull next)
        && (String.CompareOrdinal(next.Key, key) < 0 || next.Key = key && next.Seq > seq)

    let search key seq currLvl toLvl =
        let mutable pred = head

        for i = currLvl - 1 downto toLvl do
            let mutable nxt = pred.Next.[i]

            while next nxt key seq do
                pred <- nxt
                nxt <- pred.Next.[i]

        pred, pred.Next.[toLvl]

    member _.Find(key: string, snapshot: int64) =
        let currLvl = getCurrentLevel ()
        let _, current = search key snapshot currLvl 0

        if not (isNull current) && current.Key = key && current.Seq <= snapshot then
            Some current.Value
        else
            None

    member _.Put(key: string, seq: int64, ?value: string) =
        let lvl = randomLevel ()
        let mutable currLvl = getCurrentLevel ()

        while lvl > currLvl do
            Interlocked.CompareExchange(&currentLevel, lvl, currLvl) |> ignore
            currLvl <- getCurrentLevel ()

        let newNode = SkipListNode(key, seq, value, lvl)

        for i = 0 to lvl - 1 do
            let mutable success = false

            while not success do
                let pred, current = search key seq currLvl i
                newNode.Next.[i] <- current
                let actual = Interlocked.CompareExchange(&pred.Next.[i], newNode, current)

                if obj.ReferenceEquals(actual, current) then
                    success <- true

    member _.Entries() =
        let mutable entries = []
        let mutable current = head.Next.[0]

        while not (isNull current) do
            entries <- (current.Key, current.Seq, current.Value) :: entries
            current <- current.Next.[0]

        entries |> List.rev

    member _.Clear() =
        for i = 0 to MAX_LEVEL - 1 do
            head.Next.[i] <- Unchecked.defaultof<_>

        currentLevel <- 1
