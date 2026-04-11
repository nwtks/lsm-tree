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

module SkipList =
    [<Literal>]
    let MAX_LEVEL = 16

    [<Literal>]
    let P = 0.5

    [<TailCall>]
    let rec randomLevel lvl =
        if Random.Shared.NextDouble() < P && lvl < MAX_LEVEL then
            randomLevel (lvl + 1)
        else
            lvl

    let next (next: SkipListNode) key seq =
        not (isNull next)
        && (String.CompareOrdinal(next.Key, key) < 0 || next.Key = key && next.Seq > seq)

    [<TailCall>]
    let rec findPredAtLevel key seq lvl (pred: SkipListNode) =
        let nxt = pred.Next.[lvl]

        if next nxt key seq then
            findPredAtLevel key seq lvl nxt
        else
            pred

    [<TailCall>]
    let rec search head key seq toLvl lvl pred =
        if lvl < toLvl then
            pred
        else
            search head key seq toLvl (lvl - 1) (findPredAtLevel key seq lvl pred)

    [<TailCall>]
    let rec searchPreds head key seq (preds: SkipListNode[]) lvl pred =
        if lvl < 0 then
            preds
        else
            let p = findPredAtLevel key seq lvl pred
            preds.[lvl] <- p
            searchPreds head key seq preds (lvl - 1) p

    [<TailCall>]
    let rec findCurrentLevel (currentLevel: int byref) lvl =
        let currLvl = Volatile.Read(&currentLevel)

        if lvl > currLvl then
            Interlocked.CompareExchange(&currentLevel, lvl, currLvl) |> ignore
            findCurrentLevel &currentLevel lvl
        else
            currLvl

    [<TailCall>]
    let rec insertAtLevel head key seq currLvl (newNode: SkipListNode) (pred: SkipListNode) lvl =
        let current = pred.Next.[lvl]
        newNode.Next.[lvl] <- current
        let actual = Interlocked.CompareExchange(&pred.Next.[lvl], newNode, current)

        if not (obj.ReferenceEquals(actual, current)) then
            let nextPred = search head key seq lvl lvl pred
            insertAtLevel head key seq currLvl newNode nextPred lvl

    [<TailCall>]
    let rec insertAtLevels head key seq currLvl (newNode: SkipListNode) (preds: SkipListNode[]) maxLvl lvl =
        if lvl < maxLvl then
            insertAtLevel head key seq currLvl newNode preds.[lvl] lvl
            insertAtLevels head key seq currLvl newNode preds maxLvl (lvl + 1)

    [<TailCall>]
    let rec collectEntries (current: SkipListNode) acc =
        if isNull current then
            acc |> List.rev
        else
            collectEntries current.Next.[0] ((current.Key, current.Seq, current.Value) :: acc)

type SkipList() =
    let head = SkipListNode("", Int64.MaxValue, None, SkipList.MAX_LEVEL)
    let mutable currentLevel = 1

    member _.Find(key: string, snapshot: int64) =
        let currLvl = Volatile.Read(&currentLevel)
        let pred = SkipList.search head key snapshot 0 (currLvl - 1) head
        let current = pred.Next.[0]

        if not (isNull current) && current.Key = key && current.Seq <= snapshot then
            Some current.Value
        else
            None

    member _.Put(key: string, seq: int64, ?value: string) =
        let lvl = SkipList.randomLevel 1
        let currLvl = SkipList.findCurrentLevel &currentLevel lvl

        let preds =
            SkipList.searchPreds head key seq (Array.create SkipList.MAX_LEVEL head) (currLvl - 1) head

        let newNode = SkipListNode(key, seq, value, lvl)
        SkipList.insertAtLevels head key seq currLvl newNode preds lvl 0

    member _.Entries() =
        SkipList.collectEntries head.Next.[0] []
