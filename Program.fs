open System
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Linq
open System.Net
open System.Net.Http
open System.Net.Sockets
open BencodeNET.Parsing
open BencodeNET.Objects
open BencodeNET.Torrents
open Types

let encodeBytes (bytes: byte[]) =
    bytes |> Array.map (fun b -> sprintf "%%%02x" b) |> String.concat ""

let buildTrackerUrl (baseUrl: string) (parameters: TrackerParameters) =
    let query =
        [ "info_hash", encodeBytes parameters.InfoHash
          "peer_id", encodeBytes parameters.PeerId
          "port", string parameters.Port
          "uploaded", string parameters.Uploaded
          "downloaded", string parameters.Downloaded
          "left", string parameters.Left
          "compact", "1"
          "event", parameters.Event.ToString()
          "numwant", string 200 ]
        |> Seq.map (fun (k, v) -> sprintf "%s=%s" k v)
        |> String.concat "&"

    $"{baseUrl}?{query}"

let client = new HttpClient()
client.DefaultRequestHeaders.Add("User-Agent", "qBittorrent/5.1.4")

let parser = BencodeParser()

let torrent =
    parser.Parse<Torrent> "./kali-linux-2025.4-installer-amd64.iso.torrent"

let announceHttpTracker (baseUrl: string) (parameters: TrackerParameters) =
    task {
        let url = buildTrackerUrl baseUrl parameters

        printfn "%s" url

        let! response = client.GetByteArrayAsync url

        let dictionary = parser.Parse<BDictionary> response

        let peersBytes = dictionary.Get<BString>("peers").Value.ToArray()

        let peers =
            peersBytes
            |> Array.chunkBySize 6
            |> Array.filter (fun chunk -> chunk.Length = 6)
            |> Array.map (fun chunk ->
                let ip = IPAddress(chunk[0..3])
                let port = uint16 chunk[4] <<< 8 ||| uint16 chunk[5]

                { Ip = ip; Port = port })

        let trackerResponse =
            { Interval = dictionary.Get<BNumber>("interval").Value |> int
              Peers = peers }

        return trackerResponse
    }

let announce (baseUrl: string) (parameters: TrackerParameters) =
    task {
        // TODO: Implement announce to UDP tracker

        let! response = announceHttpTracker baseUrl parameters

        printfn "Peers: %i" response.Peers.Length

        return response
    }

let left (torrent: Torrent) =
    if torrent.FileMode = TorrentFileMode.Single then
        torrent.File.FileSize
    else
        torrent.Files |> Seq.sumBy (fun file -> file.FileSize)

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

let peerId = "-qB5140-kwsSnUYwydys"

printfn "%A" torrent.Trackers

let baseUrl = torrent.Trackers.Last().First()

let request =
    { InfoHash = torrent.OriginalInfoHashBytes
      PeerId = Encoding.ASCII.GetBytes peerId
      Port = 58237
      Uploaded = 0
      Downloaded = 0
      Left = left torrent
      Event = Started }

// TODO: Announce for each tracker
let response = announce baseUrl request |> Async.AwaitTask |> Async.RunSynchronously

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
