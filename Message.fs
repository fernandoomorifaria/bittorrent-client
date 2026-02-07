namespace BitTorrent.Client

open System
open System.Net

type MessageId =
    | Choke = 0
    | Unchoke = 1
    | Interested = 2
    | NotInterested = 3
    | Have = 4
    | Bitfield = 5
    | Request = 6
    | Piece = 7
    | Cancel = 8

type Message =
    | Choke
    | Unchoke
    | Interested
    | NotInterested
    | Have of pieceIndex: int
    | Bitfield of bitfield: byte[]
    | Request of pieceIndex: int * ``begin``: int * length: int
    | Piece of pieceIndex: int * ``begin``: int * block: byte[]
    | Cancel of pieceIndex: int * ``begin``: int * length: int

module Message =
    let serialize (message: Message) =
        match message with
        | Choke ->
            Array.concat
                [ BitConverter.GetBytes(IPAddress.HostToNetworkOrder 1)
                  [| byte MessageId.Choke |] ]
        | Unchoke ->
            Array.concat
                [ BitConverter.GetBytes(IPAddress.HostToNetworkOrder 1)
                  [| byte MessageId.Unchoke |] ]
        | Interested ->
            Array.concat
                [ BitConverter.GetBytes(IPAddress.HostToNetworkOrder 1)
                  [| byte MessageId.Interested |] ]
        | NotInterested ->
            Array.concat
                [ BitConverter.GetBytes(IPAddress.HostToNetworkOrder 1)
                  [| byte MessageId.NotInterested |] ]
        | Have pieceIndex ->
            let indexBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder pieceIndex)

            Array.concat
                [ BitConverter.GetBytes(IPAddress.HostToNetworkOrder 5)
                  [| byte MessageId.Have |]
                  indexBytes ]
        | Bitfield bitfield ->
            Array.concat
                [ BitConverter.GetBytes(IPAddress.HostToNetworkOrder(1 + bitfield.Length))
                  [| byte MessageId.Bitfield |]
                  bitfield ]
        | Request(pieceIndex, ``begin``, length) ->
            let indexBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder pieceIndex)
            let beginBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder ``begin``)
            let lengthBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder length)

            Array.concat
                [ BitConverter.GetBytes(IPAddress.HostToNetworkOrder 13)
                  [| byte MessageId.Request |]
                  indexBytes
                  beginBytes
                  lengthBytes ]
        | Piece(pieceIndex, ``begin``, block) ->
            let indexBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder pieceIndex)
            let beginBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder ``begin``)

            Array.concat
                [ BitConverter.GetBytes(IPAddress.HostToNetworkOrder(9 + block.Length))
                  [| byte MessageId.Piece |]
                  indexBytes
                  beginBytes
                  block ]
        | Cancel(pieceIndex, ``begin``, length) ->
            let indexBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder pieceIndex)
            let beginBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder ``begin``)
            let lengthBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder length)

            Array.concat
                [ BitConverter.GetBytes(IPAddress.HostToNetworkOrder 13)
                  [| byte MessageId.Cancel |]
                  indexBytes
                  beginBytes
                  lengthBytes ]

    let deserialize (bytes: byte[]) =
        let messageId = enum<MessageId> (int bytes.[0])

        match messageId with
        | MessageId.Choke -> Choke
        | MessageId.Unchoke -> Unchoke
        | MessageId.Interested -> Interested
        | MessageId.NotInterested -> NotInterested
        | MessageId.Have ->
            let pieceIndex = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, 1))
            Have pieceIndex
        | MessageId.Bitfield ->
            let bitfield = bytes.[1..]
            Bitfield bitfield
        | MessageId.Request ->
            let pieceIndex = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, 1))
            let ``begin`` = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, 5))
            let length = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, 9))
            Request(pieceIndex, ``begin``, length)
        | MessageId.Piece ->
            let pieceIndex = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, 1))
            let ``begin`` = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, 5))
            let block = bytes.[9..]
            Piece(pieceIndex, ``begin``, block)
        | MessageId.Cancel ->
            let pieceIndex = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, 1))
            let ``begin`` = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, 5))
            let length = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, 9))
            Cancel(pieceIndex, ``begin``, length)
