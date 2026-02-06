module Peer

open System
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Net.Sockets
open Types

let private serializeHandshake (handshake: Handshake) =
    let protocolBytes = Encoding.ASCII.GetBytes handshake.Protocol

    Array.concat
        [ [| byte protocolBytes.Length |]
          protocolBytes
          Array.zeroCreate 8
          handshake.InfoHash
          handshake.PeerId ]

let private deserializeHandshake (buffer: byte array) =
    let protocolLength = buffer.[0]
    let protocol = Encoding.ASCII.GetString(buffer, 1, int protocolLength)
    let infoHash = buffer.[28..47]
    let peerId = buffer.[48..67]

    { Protocol = protocol
      InfoHash = infoHash
      PeerId = peerId }

let private connectToPeer (peer: Peer) (handshake: Handshake) : Task<PeerConnection option> =
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
