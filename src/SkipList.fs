namespace LsmTree

open System
open System.Collections.Generic

[<AllowNullLiteral>]
type SkipListNode(key: string, seq: int64, value: string option, level: int) =
    let next = Array.zeroCreate<SkipListNode> (level)
    member val Key = key
    member val Seq = seq
    member val Value = value with get, set
    member val Next = next

type SkipList() =
    let MAX_LEVEL = 16
    let P = 0.5
    let rand = Random()

    // Sentinel node
    let head = SkipListNode("", Int64.MaxValue, None, MAX_LEVEL)
    let mutable currentLevel = 1

    let randomLevel () =
        let mutable lvl = 1

        while rand.NextDouble() < P && lvl < MAX_LEVEL do
            lvl <- lvl + 1

        lvl

    let compare (k1: string) (s1: int64) (k2: string) (s2: int64) =
        let c = String.CompareOrdinal(k1, k2)
        if c <> 0 then c else s2.CompareTo(s1) // descending seq

    member this.Find(key: string, snapshot: int64) =
        let mutable current = head

        for i = currentLevel - 1 downto 0 do
            while not (isNull current.Next.[i])
                  && (String.CompareOrdinal(current.Next.[i].Key, key) < 0
                      || (current.Next.[i].Key = key && current.Next.[i].Seq > snapshot)) do
                current <- current.Next.[i]

        current <- current.Next.[0]

        if not (isNull current) && current.Key = key && current.Seq <= snapshot then
            Some current.Value
        else
            None

    member this.Put(key: string, seq: int64, value: string option) =
        let update = Array.zeroCreate<SkipListNode> (MAX_LEVEL)
        let mutable current = head

        for i = currentLevel - 1 downto 0 do
            while not (isNull current.Next.[i])
                  && compare current.Next.[i].Key current.Next.[i].Seq key seq < 0 do
                current <- current.Next.[i]

            update.[i] <- current

        current <- current.Next.[0]

        if not (isNull current) && current.Key = key && current.Seq = seq then
            current.Value <- value
        else
            let lvl = randomLevel ()

            if lvl > currentLevel then
                for i = currentLevel to lvl - 1 do
                    update.[i] <- head

                currentLevel <- lvl

            let newNode = SkipListNode(key, seq, value, lvl)

            for i = 0 to lvl - 1 do
                newNode.Next.[i] <- update.[i].Next.[i]
                update.[i].Next.[i] <- newNode

    member this.Entries() =
        let mutable current = head.Next.[0]
        let entries = List<string * int64 * string option>()

        while not (isNull current) do
            entries.Add(current.Key, current.Seq, current.Value)
            current <- current.Next.[0]

        entries |> Seq.toList

    member this.Clear() =
        for i = 0 to MAX_LEVEL - 1 do
            head.Next.[i] <- null

        currentLevel <- 1
