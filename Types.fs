module Types

open System.Net
open System.Net.Sockets

type Event =
    | Started
    | Stopped
    | Completed

    override this.ToString() =
        match this with
        | Started -> "started"
        | Stopped -> "stopped"
        | Completed -> "completed"

type TrackerParameters =
    { InfoHash: byte array
      PeerId: byte array
      Port: int
      Uploaded: int
      Downloaded: int
      Left: int64
      Event: Event }

type Peer = { Ip: IPAddress; Port: uint16 }

type TrackerResponse =
    { // FailureReason: string option
      Interval: int
      Peers: Peer array }

type Handshake =
    { Protocol: string
      InfoHash: byte array
      PeerId: byte array }

type PeerConnection = { Peer: Peer; Connection: TcpClient }
