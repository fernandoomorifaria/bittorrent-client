namespace BitTorrent.Client

open System.Collections

type PieceWork =
    { Index: int
      Hash: byte array
      Length: int }

type CoordinatorMessage =
    | HasUsefulPieces of pieces: BitArray * replyChannel: AsyncReplyChannel<bool>
    | RequestPiece of bitfield: BitArray * replyChannel: AsyncReplyChannel<PieceWork option>
    | PieceReceived

module Coordinator =
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
