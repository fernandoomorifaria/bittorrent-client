open System
open System.IO
open System.Text
open System.Net.Http
open BencodeNET.Parsing
open BencodeNET.Torrents
open BitTorrent.Client
open System.Threading.Tasks
open Worker
open Message
open System.Threading

let calculatePieceSize (index: int) (pieceSize: int64) (totalSize: int64) =
    let beginOffset = int64 index * pieceSize
    let endOffset = beginOffset + pieceSize

    min endOffset totalSize - beginOffset |> int

let createMessageReader (connection: PeerConnection) (timeout: TimeSpan) (ct: CancellationToken) : MessageReader =
    let stream = connection.Connection.GetStream()

    fun () ->
        task {
            try
                use timeoutSource = CancellationTokenSource.CreateLinkedTokenSource ct

                timeoutSource.CancelAfter timeout

                let! message = readMessage stream timeoutSource.Token

                return Message message
            with exn ->
                return PeerDisconnected exn.Message
        }

let createMessageSender (connection: PeerConnection) (timeout: TimeSpan) (ct: CancellationToken) : MessageSender =
    let stream = connection.Connection.GetStream()

    fun (message: Message) ->
        task {
            use timeoutSource = CancellationTokenSource.CreateLinkedTokenSource ct

            timeoutSource.CancelAfter timeout

            do! sendMessage stream message timeoutSource.Token
        }

let client = new HttpClient()

client.DefaultRequestHeaders.Add("User-Agent", "qBittorrent/5.1.4")

let parser = BencodeParser()

[<Literal>]
let peerId = "-qB5140-kwsSnUYwydys"

[<Literal>]
let protocol = "BitTorrent protocol"

let torrent =
    parser.Parse<Torrent> "./kali-linux-2025.4-installer-netinst-amd64.iso.torrent"

let urls =
    torrent.Trackers
    |> Seq.collect id
    |> Seq.filter (fun url -> url.StartsWith("http", StringComparison.OrdinalIgnoreCase))
    |> Seq.map (fun url -> Uri url)
    |> Array.ofSeq

let download () =
    task {
        let request =
            { InfoHash = torrent.OriginalInfoHashBytes
              PeerId = Encoding.ASCII.GetBytes peerId
              Port = 58237
              Uploaded = 0
              Downloaded = 0
              // TODO: Calculate the left size in case of calling the tracker again
              Left = torrent.TotalSize
              Event = Started }

        let! responses = Tracker.announce urls request client

        let peers = responses |> Array.collect (fun response -> response.Peers)

        let handshake =
            { Protocol = protocol
              InfoHash = torrent.OriginalInfoHashBytes
              PeerId = Encoding.ASCII.GetBytes peerId }

        use cts = new CancellationTokenSource()

        let timeout = TimeSpan.FromSeconds 5L

        let! connections = Peer.connectToPeers peers handshake timeout cts.Token

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

        let supervisor = Supervisor.Supervisor(state, cts)

        do!
            connections
            |> Array.map (fun connection ->
                let reader = createMessageReader connection timeout cts.Token
                let sender = createMessageSender connection timeout cts.Token

                startWorker reader sender supervisor connection :> Task)
            |> Task.WhenAll
    }

download () |> Async.AwaitTask |> Async.RunSynchronously
