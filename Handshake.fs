namespace BitTorrent.Client

open System.Text

type Handshake =
    { Protocol: string
      InfoHash: byte array
      PeerId: byte array }

module Handshake =
    let serialize (handshake: Handshake) =
        let protocolBytes = Encoding.ASCII.GetBytes handshake.Protocol

        Array.concat
            [ [| byte protocolBytes.Length |]
              protocolBytes
              Array.zeroCreate 8
              handshake.InfoHash
              handshake.PeerId ]

    let deserialize (buffer: byte array) =
        let protocolLength = buffer.[0]
        let protocol = Encoding.ASCII.GetString(buffer, 1, int protocolLength)
        let infoHash = buffer.[28..47]
        let peerId = buffer.[48..67]

        { Protocol = protocol
          InfoHash = infoHash
          PeerId = peerId }
