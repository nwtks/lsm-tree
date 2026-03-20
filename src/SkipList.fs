namespace LsmTree

open System

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

    let randomLevel () =
        let mutable lvl = 1

        while rand.NextDouble() < P && lvl < MAX_LEVEL do
            lvl <- lvl + 1

        lvl

    let next (next: SkipListNode) key seq =
        not (isNull next)
        && (String.CompareOrdinal(next.Key, key) < 0 || next.Key = key && next.Seq > seq)

    let nextNode (node: SkipListNode) i key seq =
        let mutable current = node

        while next current.Next.[i] key seq do
            current <- current.Next.[i]

        current

    member _.Find(key: string, snapshot: int64) =
        let mutable search = head

        for i = currentLevel - 1 downto 0 do
            search <- nextNode search i key snapshot

        let current = search.Next.[0]

        if not (isNull current) && current.Key = key && current.Seq <= snapshot then
            Some current.Value
        else
            None

    member _.Put(key: string, seq: int64, ?value: string) =
        let addNode (update: SkipListNode[]) =
            let lvl = randomLevel ()

            if lvl > currentLevel then
                for i = currentLevel to lvl - 1 do
                    update.[i] <- head

                currentLevel <- lvl

            let newNode = SkipListNode(key, seq, value, lvl)

            for i = 0 to lvl - 1 do
                newNode.Next.[i] <- update.[i].Next.[i]
                update.[i].Next.[i] <- newNode

        let update = Array.zeroCreate<SkipListNode> MAX_LEVEL
        let mutable search = head

        for i = currentLevel - 1 downto 0 do
            search <- nextNode search i key seq
            update.[i] <- search

        let current = search.Next.[0]

        if not (isNull current) && current.Key = key && current.Seq = seq then
            current.Value <- value
        else
            addNode update

    member _.Entries() =
        let mutable entries = []
        let mutable current = head.Next.[0]

        while not (isNull current) do
            entries <- (current.Key, current.Seq, current.Value) :: entries
            current <- current.Next.[0]

        entries |> List.rev

    member _.Clear() =
        for i = 0 to MAX_LEVEL - 1 do
            head.Next.[i] <- null

        currentLevel <- 1
