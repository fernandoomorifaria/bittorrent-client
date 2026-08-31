namespace BitTorrent.Client

open System.Buffers.Binary
open System.Collections
open System.Threading.Tasks
open System.Net.Sockets
open Utils
open System.Threading

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

type Piece =
    { Index: int
      Offset: int
      Block: byte array }

type BlockRequest =
    { Index: int; Offset: int; Length: int }

type Message =
    | KeepAlive
    | Choke
    | Unchoke
    | Interested
    | NotInterested
    | Have of piece: int
    | Bitfield of bitfield: BitArray
    | Request of piece: int * offset: int * length: int
    | Piece of Piece
    | Cancel of piece: int * offset: int * length: int

type MessageResult =
    | Message of message: Message
    | PeerDisconnected of reason: string

type MessageReader = unit -> Task<MessageResult>

type MessageSender = Message -> Async<unit>

module Message =
    [<Literal>]
    let BlockSize = 16384

    [<Literal>]
    let MaxRequests = 5

    let serialize (message: Message) =
        let messageId id = [| byte id |]

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
        | Request(piece, offset, length) ->
            Array.concat
                [ writeBigEndian 13
                  messageId MessageId.Request
                  writeBigEndian piece
                  writeBigEndian offset
                  writeBigEndian length ]
        | Piece piece ->
            Array.concat
                [ writeBigEndian (9 + piece.Block.Length)
                  messageId MessageId.Piece
                  writeBigEndian piece.Index
                  writeBigEndian piece.Offset
                  piece.Block ]
        | Cancel(piece, offset, length) ->
            Array.concat
                [ writeBigEndian 13
                  messageId MessageId.Cancel
                  writeBigEndian piece
                  writeBigEndian offset
                  writeBigEndian length ]
        | KeepAlive -> writeBigEndian 0

    let deserialize (bytes: byte array) =
        let readBigEndian = readBigEndian bytes

        let messageId = enum<MessageId> (int bytes.[0])

        match messageId with
        | MessageId.Choke -> Choke
        | MessageId.Unchoke -> Unchoke
        | MessageId.Interested -> Interested
        | MessageId.NotInterested -> NotInterested
        | MessageId.Have -> Have(readBigEndian 1)
        | MessageId.Bitfield -> Bitfield(BitArray(bytes |> Array.map reverseByte))
        | MessageId.Request -> Request(readBigEndian 1, readBigEndian 5, readBigEndian 9)
        | MessageId.Piece ->
            Piece
                { Index = readBigEndian 1
                  Offset = readBigEndian 5
                  Block = bytes.[9..] }
        | MessageId.Cancel -> Cancel(readBigEndian 1, readBigEndian 5, readBigEndian 9)
        | _ -> KeepAlive

    let readMessage (stream: NetworkStream) =
        task {
            let lengthBuffer = Array.zeroCreate<byte> 4

            do! stream.ReadExactlyAsync lengthBuffer

            let length = BinaryPrimitives.ReadInt32BigEndian lengthBuffer

            if length = 0 then
                return KeepAlive
            else
                let messageBuffer = Array.zeroCreate<byte> length

                do! stream.ReadExactlyAsync messageBuffer

                return deserialize messageBuffer
        }

    let sendMessage (stream: NetworkStream) (message: Message) =
        stream.WriteAsync(serialize message).AsTask() |> Async.AwaitTask
