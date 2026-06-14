module LsmTree.Tests.WALTests

open Xunit
open LsmTree

[<Fact>]
let ``WALRecovery base64ToUtf8 decodes correctly`` () =
    let bytes = System.Text.Encoding.UTF8.GetBytes "Hello, WAL!"
    let b64 = System.Convert.ToBase64String bytes
    let decoded = WALRecovery.base64ToUtf8 b64
    assertEqual "Hello, WAL!" decoded "Valid Base64 string should decode correctly"

[<Fact>]
let ``WALRecovery base64ToUtf8 throws on invalid base64`` () =
    Assert.Throws<System.FormatException>(fun () -> WALRecovery.base64ToUtf8 "!!invalid!!" |> ignore) |> ignore

[<Fact>]
let ``WALRecovery parseEntry catches FormatException from invalid base64`` () =
    assertEqual None (WALRecovery.parseEntry "PUT 1 !!invalid!! !!base64!!") "parseEntry returns None for invalid base64"

[<Fact>]
let ``WALRecovery recover on non-existent file returns empty list`` () =
    let testDir = getTestDir "wal_non_existent"
    let path = System.IO.Path.Combine(testDir, "nonexistent.wal")
    let recovered = WALRecovery.recover path
    assertEqual Seq.empty recovered "Non-existent WAL file should return empty sequence"

[<Fact>]
let ``WALRecovery recover handles empty WAL file`` () =
    let testDir = getTestDir "wal_empty"
    let path = System.IO.Path.Combine(testDir, "data.wal")
    System.IO.File.WriteAllText(path, "")

    let recovered = WALRecovery.recover path |> Seq.toList
    assertEqual [] recovered "Empty WAL file should return empty list"

[<Fact>]
let ``WALRecovery malformed log lines are skipped`` () =
    let testDir = getTestDir "wal_malformed"
    let path = System.IO.Path.Combine(testDir, "data.wal")
    System.IO.File.WriteAllText(path, "garbage|line\n")

    let recovered = WALRecovery.recover path |> Seq.toList
    assertEqual [] recovered "Malformed lines should be skipped"

[<Fact>]
let ``WALRecovery orphaned PUT outside transaction is recovered`` () =
    let testDir = getTestDir "wal_orphan_put"
    let path = System.IO.Path.Combine(testDir, "data.wal")
    System.IO.File.WriteAllText(path, "PUT 1 a2V5MQ== dmFsMQ==")

    let recovered = WALRecovery.recover path |> Seq.toList
    let expected = [ 1L, "key1", Some "val1" ]
    assertEqual expected recovered "Orphaned PUT outside a transaction must be recovered"

[<Fact>]
let ``WALRecovery orphaned COMMIT is skipped on recovery`` () =
    let testDir = getTestDir "wal_orphan_commit"
    let path = System.IO.Path.Combine(testDir, "data.wal")
    let lines = [ "BEGIN 1"; "PUT 1 a2V5MQ== dmFsMQ=="; "COMMIT 1"; "COMMIT 2" ]
    System.IO.File.WriteAllLines(path, lines)

    let recovered = WALRecovery.recover path |> Seq.toList
    let expected = [ 1L, "key1", Some "val1" ]
    assertEqual expected recovered "Orphaned COMMIT should be ignored, valid txn recovered"

[<Fact>]
let ``WALRecovery unknown entry types are skipped`` () =
    let testDir = getTestDir "wal_unknown"
    let path = System.IO.Path.Combine(testDir, "data.wal")
    let lines = [ "BEGIN 1"; "UNKNOWN foo bar"; "PUT 1 a2V5MQ== dmFsMQ=="; "COMMIT 1" ]
    System.IO.File.WriteAllLines(path, lines)

    let recovered = WALRecovery.recover path |> Seq.toList
    let expected = [ 1L, "key1", Some "val1" ]
    assertEqual expected recovered "Unknown entry types should be gracefully skipped"

[<Fact>]
let ``WALRecovery DEL entries are recovered as tombstones`` () =
    let testDir = getTestDir "wal_del_entries"
    let path = System.IO.Path.Combine(testDir, "data.wal")

    let lines =
        [ "BEGIN 1"
          "PUT 1 a2V5MQ== dmFsMQ=="
          "PUT 1 a2V5Mg== dmFsMg=="
          "DEL 1 a2V5MQ=="
          "COMMIT 1" ]

    System.IO.File.WriteAllLines(path, lines)
    let recovered = WALRecovery.recover path |> Seq.toList

    let expected =
        [ 1L, "key1", Some "val1"; 1L, "key2", Some "val2"; 1L, "key1", None ]

    assertEqual expected recovered "DEL entries should be recovered as tombstones along with all committed ops"

[<Fact>]
let ``WALRecovery uncommitted transactions are excluded on recovery`` () =
    let testDir = getTestDir "wal_atomic"
    let path = System.IO.Path.Combine(testDir, "data.wal")

    let lines =
        [ "BEGIN 1"
          "PUT 1 a2V5MQ== dmFsMQ=="
          "BEGIN 2"
          "PUT 2 a2V5Mg== dmFsMg=="
          "COMMIT 2" ]

    System.IO.File.WriteAllLines(path, lines)
    let recovered = WALRecovery.recover path |> Seq.toList
    let expected = [ 2L, "key2", Some "val2" ]
    assertEqual expected recovered "Only committed transactions should survive recovery"

[<Fact>]
let ``WAL Put and recover restores data correctly`` () =
    let testDir = getTestDir "wal_restore"
    let path = System.IO.Path.Combine(testDir, "data.wal")

    do
        use wal = new WAL(path)
        wal.Put(1L, "key1", "val1")
        wal.Put(2L, "key2", "val2")
        wal.Close()

    let recovered = WALRecovery.recover path |> Seq.toList
    let expected = [ 1L, "key1", Some "val1"; 2L, "key2", Some "val2" ]
    assertEqual expected recovered "WAL data should be recovered correctly after Close"

[<Fact>]
let ``WAL IO errors are propagated to the caller`` () =
    let testDir = getTestDir "wal_io_errors"
    let path = System.IO.Path.Combine(testDir, "data.wal")
    use wal = new WAL(path)

    let handleField =
        typeof<WAL>
            .GetField(
                "stream",
                System.Reflection.BindingFlags.NonPublic
                ||| System.Reflection.BindingFlags.Instance
            )

    let fs = handleField.GetValue wal :?> System.IO.FileStream
    fs.Close()

    wal.Put(1L, "k2", "v2")

    Assert.Throws<System.ObjectDisposedException>(fun () -> wal.PutSingle(2L, "k3", "v3", true))
    |> ignore

[<Fact>]
let ``WAL double dispose does not throw`` () =
    let testDir = getTestDir "wal_double_dispose"
    let path = System.IO.Path.Combine(testDir, "data.wal")

    let wal = new WAL(path)
    wal.Close()
    (wal :> System.IDisposable).Dispose()

[<Fact>]
let ``WALRecovery parsePut returns None when parts length is not 4`` () =
    assertEqual None (WALRecovery.parsePut 1L [|"PUT"; "1"; "a2V5MQ=="|]) "PUT with 3 parts returns None"

[<Fact>]
let ``WALRecovery parsePut decodes base64 key and value`` () =
    let expected = Some(1L, WALRecovery.RecoveryEntry.Op("key1", Some "val1"))
    assertEqual expected (WALRecovery.parsePut 1L [|"PUT"; "1"; "a2V5MQ=="; "dmFsMQ=="|]) "PUT with 4 parts and valid base64 succeeds"

[<Fact>]
let ``WALRecovery parseDel returns None when parts length is not 3`` () =
    assertEqual None (WALRecovery.parseDel 1L [|"DEL"; "1"; "a2V5MQ=="; "dmFsMQ=="|]) "DEL with 4 parts returns None"

[<Fact>]
let ``WALRecovery parseDel decodes base64 key with tombstone value`` () =
    let expected = Some(1L, WALRecovery.RecoveryEntry.Op("key1", None))
    assertEqual expected (WALRecovery.parseDel 1L [|"DEL"; "1"; "a2V5MQ=="|]) "DEL with 3 parts and valid base64 succeeds"

[<Fact>]
let ``WALRecovery parseBeginCommit returns None when parts length is not 2`` () =
    assertEqual None (WALRecovery.parseBeginCommit 1L [|"BEGIN"; "1"; "extra"|] WALRecovery.RecoveryEntry.Begin) "BEGIN with 3 parts returns None"
    assertEqual None (WALRecovery.parseBeginCommit 1L [|"COMMIT"; "1"; "extra"|] WALRecovery.RecoveryEntry.Commit) "COMMIT with 3 parts returns None"

[<Fact>]
let ``WALRecovery parseBeginCommit returns entry for 2 parts`` () =
    assertEqual (Some(1L, WALRecovery.RecoveryEntry.Begin)) (WALRecovery.parseBeginCommit 1L [|"BEGIN"; "1"|] WALRecovery.RecoveryEntry.Begin) "BEGIN with 2 parts succeeds"
    assertEqual (Some(1L, WALRecovery.RecoveryEntry.Commit)) (WALRecovery.parseBeginCommit 1L [|"COMMIT"; "1"|] WALRecovery.RecoveryEntry.Commit) "COMMIT with 2 parts succeeds"
