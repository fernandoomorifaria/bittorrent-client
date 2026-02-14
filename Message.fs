namespace BitTorrent.Client

open System
open System.Buffers.Binary
open System.Collections
open Peer

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
    | Have of piece: int
    | Bitfield of bitfield: BitArray
    | Request of piece: int * ``begin``: int * length: int
    | Piece of piece: int * ``begin``: int * block: byte array
    | Cancel of piece: int * ``begin``: int * length: int

module Message =
    [<Literal>]
    let BlockSize = 16384

    let serialize (message: Message) =
        let writeBigEndian value =
            let bytes = Array.zeroCreate 4
            BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(), value)
            bytes

        let messageId id = [| byte id |]

        let bitArrayToBytes (bitArray: BitArray) =
            let byteArray = Array.zeroCreate ((bitArray.Length + 7) / 8)
            bitArray.CopyTo(byteArray, 0)
            byteArray

        match message with
        | Choke -> Array.concat [ writeBigEndian 1; messageId MessageId.Choke ]
        | Unchoke -> Array.concat [ writeBigEndian 1; messageId MessageId.Unchoke ]
        | Interested -> Array.concat [ writeBigEndian 1; messageId MessageId.Interested ]
        | NotInterested -> Array.concat [ writeBigEndian 1; messageId MessageId.NotInterested ]
        | Have piece -> Array.concat [ writeBigEndian 5; messageId MessageId.Have; writeBigEndian piece ]
        | Bitfield bitfield ->
            let bitfieldBytes = bitArrayToBytes bitfield

            Array.concat
                [ writeBigEndian (1 + bitfieldBytes.Length)
                  messageId MessageId.Bitfield
                  bitfieldBytes ]
        | Request(piece, ``begin``, length) ->
            Array.concat
                [ writeBigEndian 13
                  messageId MessageId.Request
                  writeBigEndian piece
                  writeBigEndian ``begin``
                  writeBigEndian length ]
        | Piece(piece, ``begin``, block) ->
            Array.concat
                [ writeBigEndian (9 + block.Length)
                  messageId MessageId.Piece
                  writeBigEndian piece
                  writeBigEndian ``begin``
                  block ]
        | Cancel(piece, ``begin``, length) ->
            Array.concat
                [ writeBigEndian 13
                  messageId MessageId.Cancel
                  writeBigEndian piece
                  writeBigEndian ``begin``
                  writeBigEndian length ]

    let deserialize (bytes: byte array) =
        let readBigEndian offset =
            BinaryPrimitives.ReadInt32BigEndian(bytes.[offset .. offset + 3])

        let messageId = enum<MessageId> (int bytes.[4])

        match messageId with
        | MessageId.Choke -> Choke
        | MessageId.Unchoke -> Unchoke
        | MessageId.Interested -> Interested
        | MessageId.NotInterested -> NotInterested
        | MessageId.Have -> Have(readBigEndian 5)
        | MessageId.Bitfield -> Bitfield(BitArray(bytes[5..]))
        | MessageId.Request -> Request(readBigEndian 5, readBigEndian 9, readBigEndian 13)
        | MessageId.Piece -> Piece(readBigEndian 5, readBigEndian 9, bytes[13..])
        | MessageId.Cancel -> Cancel(readBigEndian 5, readBigEndian 9, readBigEndian 13)
        | _ -> failwithf "%i" (int bytes.[4])

    let calculatePieceSize (index: int) (state: State) =
        let ``begin`` = index * BlockSize

        if index = state.NumberOfPieces - 1 then
            ``begin``, state.PieceSize - ``begin``
        else
            ``begin``, BlockSize

    let processMessage (peer: PeerConnection) (message: Message) (state: State) : PeerConnection * PeerAction option =
        (*task {
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
        }*)

        match message with
        // NOTE: Maybe I should return the request with the actual request list
        | Unchoke -> { peer with PeerChoking = false }, Some RequestBlocks
        | Bitfield bitfield ->
            let withBitfield = { peer with Bitfield = Some bitfield }

            if hasPiecesWeNeed bitfield state.Pieces then
                { withBitfield with
                    AmInterested = true },
                Some SendInterested
            else
                withBitfield, None
        | Have piece ->
            // TODO: Reply with INTERESTED
            match peer.Bitfield with
            | None ->
                let bitfield = BitArray state.NumberOfPieces

                bitfield.Set(piece, true)

                let withBitfield = { peer with Bitfield = Some bitfield }

                withBitfield, None
            | Some bitfield ->
                bitfield.Set(piece, true)

                peer, None
