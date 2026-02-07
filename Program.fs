open System.Linq
open System.Text
open BencodeNET.Parsing
open BencodeNET.Torrents
open BitTorrent.Client

let parser = BencodeParser()

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
    Peer.connectToPeers response.Peers handshake
    |> Async.AwaitTask
    |> Async.RunSynchronously

printfn "Connections: %i" connections.Length

// TODO: Implement message loop, for now just close the connections
connections |> Array.iter (fun peer -> peer.Connection.Close())
