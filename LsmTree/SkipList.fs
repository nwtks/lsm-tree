namespace LsmTree

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

    let randomLevel () =
        let bits = System.Random.Shared.Next()
        let mutable lvl = 1
        let mutable mask = 1

        while lvl < MAX_LEVEL && bits &&& mask <> 0 do
            lvl <- lvl + 1
            mask <- mask <<< 1

        lvl

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

    [<TailCall>]
    let rec insertAtLevel head key seq (newNode: SkipListNode) (pred: SkipListNode) lvl =
        let current = pred.Next.[lvl]
        newNode.Next.[lvl] <- current

        let actual =
            System.Threading.Interlocked.CompareExchange(&pred.Next.[lvl], newNode, current)

        if not (obj.ReferenceEquals(actual, current)) then
            let nextPred = search head key seq lvl lvl pred
            insertAtLevel head key seq newNode nextPred lvl

    [<TailCall>]
    let rec insertAtLevels head key seq (newNode: SkipListNode) (preds: SkipListNode[]) maxLvl lvl =
        if lvl < maxLvl then
            insertAtLevel head key seq newNode preds.[lvl] lvl
            insertAtLevels head key seq newNode preds maxLvl (lvl + 1)

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
        let pred = SkipList.search head key snapshot 0 (currLvl - 1) head
        let current = System.Threading.Volatile.Read(&pred.Next.[0])

        if not (isNull current) && current.Key = key && current.Seq <= snapshot then
            Some current.Value
        else
            None

    member _.Put(key: string, seq: int64, ?value: string) =
        let lvl = SkipList.randomLevel ()
        let currLvl = SkipList.findCurrentLevel &currentLevel lvl

        let preds =
            SkipList.searchPreds head key seq (Array.create SkipList.MAX_LEVEL head) (currLvl - 1) head

        let newNode = SkipListNode(key, seq, value, lvl)
        SkipList.insertAtLevels head key seq newNode preds lvl 0

    member _.Entries() =
        SkipList.collectEntries head.Next.[0] []
