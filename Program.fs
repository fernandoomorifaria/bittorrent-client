open System
open System.IO
open System.Linq
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Net.Sockets
open BencodeNET.Parsing
open BencodeNET.Torrents
open Types

let parser = BencodeParser()

let serializeHandshake (handshake: Handshake) =
    let protocolBytes = Encoding.ASCII.GetBytes handshake.Protocol

    Array.concat
        [ [| byte protocolBytes.Length |]
          protocolBytes
          Array.zeroCreate 8
          handshake.InfoHash
          handshake.PeerId ]

let deserializeHandshake (buffer: byte array) =
    let protocolLength = buffer.[0]
    let protocol = Encoding.ASCII.GetString(buffer, 1, int protocolLength)
    let infoHash = buffer.[28..47]
    let peerId = buffer.[48..67]

    { Protocol = protocol
      InfoHash = infoHash
      PeerId = peerId }

let connectToPeer (peer: Peer) (handshake: Handshake) : Task<PeerConnection option> =
    task {
        let client = new TcpClient()

        client.ReceiveTimeout <- 5_000
        client.SendTimeout <- 5_000

        use cts = new CancellationTokenSource 5_000

        try
            do! client.ConnectAsync(peer.Ip, peer.Port |> int, cts.Token)

            let stream = client.GetStream()

            let bytes = serializeHandshake handshake

            do! stream.WriteAsync bytes

            let! buffer = stream.AsyncRead 68

            let response = deserializeHandshake buffer

            if handshake.InfoHash <> response.InfoHash then
                client.Close()

                return None
            else
                printfn "Connected to %s" (peer.Ip.ToString())

                return Some { Connection = client; Peer = peer }
        with
        | :? SocketException
        | :? IOException
        | :? OperationCanceledException ->
            client.Close()

            printfn "Failed to connect to peer %s" (peer.Ip.ToString())

            return None
    }

let connectToPeers (peers: Peer array) (handshake: Handshake) =
    task {
        let! results = peers |> Array.map (fun peer -> connectToPeer peer handshake) |> Task.WhenAll

        let connections = Array.choose id results

        return connections
    }

[<Literal>]
let peerId = "-qB5140-kwsSnUYwydys"

let torrent =
    parser.Parse<Torrent> "./kali-linux-2025.4-installer-amd64.iso.torrent"

printfn "%A" torrent.Trackers

// TODO: Announce for each tracker
let baseUrl = torrent.Trackers.Last().First()

let request =
    { InfoHash = torrent.OriginalInfoHashBytes
      PeerId = Encoding.ASCII.GetBytes peerId
      Port = 58237
      Uploaded = 0
      Downloaded = 0
      Left = Utils.getBytesRemaining torrent
      Event = Started }

let response =
    Tracker.announce baseUrl request |> Async.AwaitTask |> Async.RunSynchronously

let handshake =
    { Protocol = "BitTorrent protocol"
      InfoHash = torrent.OriginalInfoHashBytes
      PeerId = Encoding.ASCII.GetBytes peerId }

let connections =
    connectToPeers response.Peers handshake
    |> Async.AwaitTask
    |> Async.RunSynchronously

printfn "Connections: %i" connections.Length

// TODO: Implement message loop, for now just close the connections
connections |> Array.iter (fun peer -> peer.Connection.Close())
