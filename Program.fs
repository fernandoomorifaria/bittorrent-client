open System
open System.IO
open System.Linq
open System.Text
open System.Net.Http
open BencodeNET.Parsing
open BencodeNET.Torrents
open BitTorrent.Client
open System.Threading.Tasks
open Coordinator
open Worker
open System.Net.Sockets
open Message
open System.Threading

let calculatePieceSize (index: int) (pieceSize: int64) (totalSize: int64) =
    let beginOffset = int64 index * pieceSize
    let endOffset = beginOffset + pieceSize

    min endOffset totalSize - beginOffset |> int

let createMessageReader (connection: PeerConnection) : MessageReader =
    let stream = connection.Connection.GetStream()

    fun () -> readMessage stream

let startWorker (reader: MessageReader) (worker: Worker) (ct: CancellationToken) =
    task {
        while true do
            let! message = reader ()

            worker.ProcessMessage message
    }

let client = new HttpClient()

client.DefaultRequestHeaders.Add("User-Agent", "qBittorrent/5.1.4")

let parser = BencodeParser()

[<Literal>]
let peerId = "-qB5140-kwsSnUYwydys"

let torrent =
    parser.Parse<Torrent> "./kali-linux-2025.4-installer-netinst-amd64.iso.torrent"

printfn "%A" torrent.Trackers

// TODO: Announce for each tracker
let baseUrl = torrent.Trackers.Last().First()

let request =
    { InfoHash = torrent.OriginalInfoHashBytes
      PeerId = Encoding.ASCII.GetBytes peerId
      Port = 58237
      Uploaded = 0
      Downloaded = 0
      // TODO: Calculate the left size in case of calling the tracker again
      Left = torrent.TotalSize
      Event = Started }

// TODO: Combine the operations into a single pipeline
let response =
    Tracker.announce baseUrl request client
    |> Async.AwaitTask
    |> Async.RunSynchronously

let handshake =
    { Protocol = "BitTorrent protocol"
      InfoHash = torrent.OriginalInfoHashBytes
      PeerId = Encoding.ASCII.GetBytes peerId }

let connections =
    Peer.connectToPeers response.Peers handshake
    |> Async.AwaitTask
    |> Async.RunSynchronously

printfn "Connections: %i" connections.Length

let pieces =
    torrent.Pieces
    |> Array.chunkBySize 20
    |> Array.mapi (fun index pieceHash ->
        { Index = index
          Hash = pieceHash
          Length = calculatePieceSize index torrent.PieceSize torrent.TotalSize })

let fileName = torrent.File.FileName

let stream =
    new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None)

let state =
    { Stream = stream
      Pieces = pieces
      PieceSize = torrent.PieceSize }

let cts = new CancellationTokenSource()

let coordinator = Coordinator(state, cts)

connections
|> Array.map (fun connection ->
    let reader = createMessageReader connection
    let worker = Worker(connection, coordinator)

    startWorker reader worker cts.Token)
|> Task.WhenAll
|> Async.AwaitTask
|> Async.RunSynchronously
|> ignore

// TODO: Implement message loop, for now just close the connections
connections |> Array.iter (fun peer -> peer.Connection.Close())
