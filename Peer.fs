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
    let connectToPeer (peer: Peer) (handshake: Handshake) : Task<PeerConnection option> =
        task {
            let client = new TcpClient()

            client.ReceiveTimeout <- 5_000
            client.SendTimeout <- 5_000

            use cts = new CancellationTokenSource 5_000

            try
                do! client.ConnectAsync(peer.Ip, peer.Port |> int, cts.Token)

                let stream = client.GetStream()

                let bytes = Handshake.serialize handshake

                do! stream.WriteAsync bytes

                let! buffer = stream.AsyncRead 68

                let response = Handshake.deserialize buffer

                if handshake.InfoHash <> response.InfoHash then
                    client.Close()

                    return None
                else
                    printfn "Connected to %s" (peer.Ip.ToString())

                    return
                        Some
                            { Connection = client
                              Peer = peer
                              Bitfield = None
                              AmChoking = true
                              AmInterested = false
                              PeerChoking = true
                              PeerInterested = false }
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
