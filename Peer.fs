namespace BitTorrent.Client

open System
open System.Collections
open System.IO
open System.Threading
open System.Threading.Tasks
open System.Net
open System.Net.Sockets

type Peer = { Ip: IPAddress; Port: uint16 }

type PeerConnection =
    { Peer: Peer
      Connection: TcpClient
      Bitfield: BitArray option
      // Here I stand, slowly choking
      AmChoking: bool
      AmInterested: bool
      PeerChoking: bool
      PeerInterested: bool }

module Peer =
    let connectToPeer
        (peer: Peer)
        (handshake: Handshake)
        (timeout: TimeSpan)
        (ct: CancellationToken)
        : Task<PeerConnection option> =
        task {
            let client = new TcpClient()

            try
                use timeoutSource = CancellationTokenSource.CreateLinkedTokenSource ct

                timeoutSource.CancelAfter timeout

                do! client.ConnectAsync(peer.Ip, peer.Port |> int, timeoutSource.Token)

                let stream = client.GetStream()

                let bytes = Handshake.serialize handshake

                do! stream.WriteAsync(bytes, timeoutSource.Token)

                let handshakeResponseBuffer = Array.zeroCreate<byte> 68

                do! stream.ReadExactlyAsync(handshakeResponseBuffer, timeoutSource.Token)

                let response = Handshake.deserialize handshakeResponseBuffer

                if handshake.InfoHash <> response.InfoHash then
                    client.Close()

                    return None
                else
                    return
                        Some
                            { Connection = client
                              Peer = peer
                              Bitfield = None
                              AmChoking = true
                              AmInterested = false
                              PeerChoking = true
                              PeerInterested = false }
            with exn ->
                printfn "Failed to connect to peer %s: %s" (peer.Ip.ToString()) exn.Message

                client.Close()

                return None
        }

    let connectToPeers (peers: Peer array) (handshake: Handshake) (timeout: TimeSpan) (ct: CancellationToken) =
        task {
            let! results =
                peers
                |> Array.map (fun peer -> connectToPeer peer handshake timeout ct)
                |> Task.WhenAll

            let connections = Array.choose id results

            return connections
        }
