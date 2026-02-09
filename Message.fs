namespace BitTorrent.Client

open System
open System.Buffers.Binary

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
    let private serialize (message: Message) =
        let writeInt32BigEndian value =
            let bytes = Array.zeroCreate 4
            BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(), value)
            bytes

        let messageId id = [| byte id |]

        match message with
        | Choke -> Array.concat [ writeInt32BigEndian 1; messageId MessageId.Choke ]
        | Unchoke -> Array.concat [ writeInt32BigEndian 1; messageId MessageId.Unchoke ]
        | Interested -> Array.concat [ writeInt32BigEndian 1; messageId MessageId.Interested ]
        | NotInterested -> Array.concat [ writeInt32BigEndian 1; messageId MessageId.NotInterested ]
        | Have pieceIndex ->
            Array.concat
                [ writeInt32BigEndian 5
                  messageId MessageId.Have
                  writeInt32BigEndian pieceIndex ]
        | Bitfield bitfield ->
            Array.concat
                [ writeInt32BigEndian (1 + bitfield.Length)
                  messageId MessageId.Bitfield
                  bitfield ]
        | Request(pieceIndex, ``begin``, length) ->
            Array.concat
                [ writeInt32BigEndian 13
                  messageId MessageId.Request
                  writeInt32BigEndian pieceIndex
                  writeInt32BigEndian ``begin``
                  writeInt32BigEndian length ]
        | Piece(pieceIndex, ``begin``, block) ->
            Array.concat
                [ writeInt32BigEndian (9 + block.Length)
                  messageId MessageId.Piece
                  writeInt32BigEndian pieceIndex
                  writeInt32BigEndian ``begin``
                  block ]
        | Cancel(pieceIndex, ``begin``, length) ->
            Array.concat
                [ writeInt32BigEndian 13
                  messageId MessageId.Cancel
                  writeInt32BigEndian pieceIndex
                  writeInt32BigEndian ``begin``
                  writeInt32BigEndian length ]

    let private deserialize (bytes: byte[]) =
        let readBigEndian offset =
            BinaryPrimitives.ReadInt32BigEndian(bytes.[offset .. offset + 3])

        let messageId = enum<MessageId> (int bytes.[0])

        match messageId with
        | MessageId.Choke -> Choke
        | MessageId.Unchoke -> Unchoke
        | MessageId.Interested -> Interested
        | MessageId.NotInterested -> NotInterested
        | MessageId.Have -> Have(readBigEndian 1)
        | MessageId.Bitfield -> Bitfield bytes[1..]
        | MessageId.Request -> Request(readBigEndian 1, readBigEndian 5, readBigEndian 9)
        | MessageId.Piece -> Piece(readBigEndian 1, readBigEndian 5, bytes[9..])
        | MessageId.Cancel -> Cancel(readBigEndian 1, readBigEndian 5, readBigEndian 9)
        | _ -> failwithf "%i" (int bytes.[0])

    let rec processMessage (peer: PeerConnection) =
        task {
            let stream = peer.Connection.GetStream()

            let lengthBuffer = Array.zeroCreate<byte> 4

            // TODO: Use CancellationToken
            let! _ = stream.ReadAsync lengthBuffer

            let length = BinaryPrimitives.ReadInt32BigEndian lengthBuffer

            if length = 0 then
                return! processMessage peer
            else
                let messageBuffer = Array.zeroCreate<byte> length

                let! _ = stream.ReadAsync messageBuffer

                let message = deserialize messageBuffer

                let connection =
                    match message with
                    | Choke -> { peer with PeerChoking = true }
                    | Unchoke -> { peer with PeerChoking = false }


                return! processMessage connection
        }
