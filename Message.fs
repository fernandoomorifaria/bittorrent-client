namespace BitTorrent.Client

open System
open System.Buffers.Binary
open System.Collections
open System.Net.Sockets
open System.Threading.Tasks

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
    | Request of piece: int * offset: int * length: int
    | Piece of piece: int * offset: int * block: byte array
    | Cancel of piece: int * offset: int * length: int

type CoordinatorMessage =
    | HasUsefulPieces of pieces: BitArray * replyChannel: AsyncReplyChannel<bool>
    | RequestPiece of bitfield: BitArray * replyChannel: AsyncReplyChannel<PieceWork option>
    | PieceReceived

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
        | Request(piece, offset, length) ->
            Array.concat
                [ writeBigEndian 13
                  messageId MessageId.Request
                  writeBigEndian piece
                  writeBigEndian offset
                  writeBigEndian length ]
        | Piece(piece, offset, block) ->
            Array.concat
                [ writeBigEndian (9 + block.Length)
                  messageId MessageId.Piece
                  writeBigEndian piece
                  writeBigEndian offset
                  block ]
        | Cancel(piece, offset, length) ->
            Array.concat
                [ writeBigEndian 13
                  messageId MessageId.Cancel
                  writeBigEndian piece
                  writeBigEndian offset
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
        let beginOffset = int64 index * state.PieceSize
        let endOffset = beginOffset + state.PieceSize

        min endOffset state.TotalSize - beginOffset

    type Coordinator(pieces: PieceWork list) =
        let agent =
            MailboxProcessor<CoordinatorMessage>.Start(fun inbox ->
                let rec loop (remainingPieces: PieceWork list) =
                    async {
                        let! message = inbox.Receive()

                        let nextPieces =
                            match message with
                            | HasUsefulPieces(pieces, replyChannel) ->
                                remainingPieces
                                |> List.exists (fun work -> pieces.[work.Index])
                                |> replyChannel.Reply

                                remainingPieces
                            | RequestPiece(bitfield, replyChannel) ->
                                let piece = remainingPieces |> List.tryFind (fun work -> bitfield.[work.Index])

                                replyChannel.Reply piece

                                match piece with
                                | Some found -> remainingPieces |> List.filter (fun piece -> piece <> found)
                                | None -> remainingPieces

                        return! loop nextPieces
                    }

                loop pieces)

        member _.HasUsefulPieces(pieces: BitArray) =
            agent.PostAndAsyncReply(fun channel -> HasUsefulPieces(pieces, channel))

        member _.RequestPiece(bitfield: BitArray) =
            agent.PostAndAsyncReply(fun channel -> RequestPiece(bitfield, channel))

    type PieceProgress = { Piece: PieceWork; Requests: int }

    let sendRequests (connection: PeerConnection) (PieceProgress: PieceProgress) = ()

    type Worker(connection: PeerConnection, coordinator: Coordinator) =
        let agent =
            MailboxProcessor<Message>.Start(fun inbox ->
                let rec loop (connection: PeerConnection) (pieceProgress: PieceProgress option) =
                    async {
                        let! message = inbox.Receive()

                        match message with
                        | Unchoke ->
                            let unchoked = { connection with AmChoking = false }

                            let! piece = coordinator.RequestPiece connection.Bitfield.Value

                            // TODO: Start piece download
                            return! loop unchoked pieceProgress
                        | Bitfield bitfield ->
                            let withBitfield =
                                { connection with
                                    Bitfield = Some bitfield }

                            let! hasUsefulPieces = coordinator.HasUsefulPieces bitfield

                            if hasUsefulPieces then
                                return!
                                    loop
                                        { withBitfield with
                                            AmInterested = true }
                                        pieceProgress
                            else
                                return! loop withBitfield pieceProgress

                        ()
                    }

                loop connection None)

        member _.ProcessMessage(message: Message) = agent.Post message

    let readMessage (stream: NetworkStream) =
        task {
            let lengthBuffer = Array.zeroCreate<byte> 4

            // TODO: Use CancellationToken
            let! _ = stream.ReadAsync lengthBuffer

            let length = BinaryPrimitives.ReadInt32BigEndian lengthBuffer

            if length = 0 then
                // NOTE: Maybe I should add KeepAlive to Message
                return None
            else
                let messageBuffer = Array.zeroCreate<byte> length

                let! _ = stream.ReadAsync messageBuffer

                let message = deserialize messageBuffer

                return Some message
        }

    let rec foo (readMessage: unit -> Task<Message option>) (worker: Worker) =
        task {
            let! message = readMessage ()

            match message with
            | None -> return! foo readMessage worker
            | Some message ->
                worker.ProcessMessage message

                return! foo readMessage worker

        }
