namespace LsmTree

[<AllowNullLiteral>]
type SkipListNode(key: string, seq: int64, value: string option, level: int) =
    let next = Array.zeroCreate<SkipListNode> level
    member val Key = key
    member val Seq = seq
    member val Value = value with get, set
    member val Next = next

[<Struct>]
type SearchResult =
    | Found of value: string
    | Tombstone
    | NotFound

module SkipList =
    [<Literal>]
    let MAX_LEVEL = 16

    let randomLevel () =
        let bits = System.Random.Shared.Next()
        let mutable lvl = 1
        let mutable mask = 1

        while lvl < MAX_LEVEL && bits &&& mask <> 0 do
            lvl <- lvl + 1
            mask <- mask <<< 1

        lvl

    let findCurrentLevel (currentLevel: int byref) lvl =
        let mutable currLvl = System.Threading.Volatile.Read(&currentLevel)

        while lvl > currLvl do
            let actual =
                System.Threading.Interlocked.CompareExchange(&currentLevel, lvl, currLvl)

            if actual = currLvl then
                currLvl <- lvl
            else
                currLvl <- actual

        currLvl

    let next (next: SkipListNode) key seq =
        not (isNull next)
        && (System.String.CompareOrdinal(next.Key, key) < 0
            || next.Key = key && next.Seq > seq)

    [<TailCall>]
    let rec findPredAtLevel key seq lvl (pred: SkipListNode) =
        let nxt = System.Threading.Volatile.Read(&pred.Next.[lvl])

        if next nxt key seq then
            findPredAtLevel key seq lvl nxt
        else
            pred

    [<TailCall>]
    let rec search key seq stopAtLvl lvl pred =
        if lvl < stopAtLvl then
            pred
        else
            search key seq stopAtLvl (lvl - 1) (findPredAtLevel key seq lvl pred)

    [<TailCall>]
    let rec searchPreds key seq (preds: SkipListNode[]) lvl pred =
        if lvl < 0 then
            preds
        else
            let p = findPredAtLevel key seq lvl pred
            preds.[lvl] <- p
            searchPreds key seq preds (lvl - 1) p

    [<TailCall>]
    let rec insertAtLevel key seq (newNode: SkipListNode) (pred: SkipListNode) lvl =
        let current = pred.Next.[lvl]
        newNode.Next.[lvl] <- current

        let actual =
            System.Threading.Interlocked.CompareExchange(&pred.Next.[lvl], newNode, current)

        if not (obj.ReferenceEquals(actual, current)) then
            let nextPred = search key seq lvl lvl pred
            insertAtLevel key seq newNode nextPred lvl

    [<TailCall>]
    let rec insertAtLevels key seq (newNode: SkipListNode) (preds: SkipListNode[]) maxLvl lvl =
        if lvl < maxLvl then
            insertAtLevel key seq newNode preds.[lvl] lvl
            insertAtLevels key seq newNode preds maxLvl (lvl + 1)

    [<TailCall>]
    let rec collectEntries (current: SkipListNode) acc =
        if isNull current then
            acc |> List.rev
        else
            let next = System.Threading.Volatile.Read(&current.Next.[0])
            collectEntries next ((current.Key, current.Seq, current.Value) :: acc)

type SkipList() =
    let head = SkipListNode("", System.Int64.MaxValue, None, SkipList.MAX_LEVEL)
    let mutable currentLevel = 1

    member _.Find(key: string, snapshot: int64) =
        let currLvl = System.Threading.Volatile.Read(&currentLevel)
        let pred = SkipList.search key snapshot 0 (currLvl - 1) head
        let current = System.Threading.Volatile.Read(&pred.Next.[0])

        if not (isNull current) && current.Key = key && current.Seq <= snapshot then
            match current.Value with
            | Some v -> Found v
            | None -> Tombstone
        else
            NotFound

    member _.Put(key: string, seq: int64, ?value: string) =
        let lvl = SkipList.randomLevel ()
        let currLvl = SkipList.findCurrentLevel &currentLevel lvl

        let preds =
            SkipList.searchPreds key seq (Array.create SkipList.MAX_LEVEL head) (currLvl - 1) head

        let newNode = SkipListNode(key, seq, value, lvl)
        SkipList.insertAtLevels key seq newNode preds lvl 0

    member _.Entries() =
        SkipList.collectEntries head.Next.[0] []
