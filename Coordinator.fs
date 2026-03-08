namespace BitTorrent.Client

open System.Threading
open System.Collections
open System.IO

type PieceWork =
    { Index: int
      Hash: byte array
      Length: int }

type State =
    { Stream: FileStream
      Pieces: PieceWork array
      PieceSize: int64 }

type CoordinatorMessage =
    | HasUsefulPieces of pieces: BitArray * replyChannel: AsyncReplyChannel<bool>
    | RequestPiece of bitfield: BitArray * replyChannel: AsyncReplyChannel<PieceWork option>
    | PieceReceived of index: int * data: byte array
    | PieceFailed of PieceWork

module Coordinator =
    type Coordinator(state: State, cts: CancellationTokenSource) =
        let pieceMap = state.Pieces |> Array.map (fun p -> p.Index, p) |> Map.ofArray

        let agent =
            MailboxProcessor<CoordinatorMessage>.Start(fun inbox ->
                let rec loop (remainingPieces: Map<int, PieceWork>) (completed: int) =
                    async {
                        let! message = inbox.Receive()

                        let stream = state.Stream

                        let nextPieces, completed =
                            match message with
                            | HasUsefulPieces(pieces, replyChannel) ->
                                remainingPieces
                                |> Map.exists (fun index _ -> pieces.[index])
                                |> replyChannel.Reply

                                remainingPieces, completed

                            | RequestPiece(bitfield, replyChannel) ->
                                let piece = remainingPieces |> Map.tryFindKey (fun index _ -> bitfield.[index])
                                replyChannel.Reply(piece |> Option.map (fun i -> remainingPieces.[i]))

                                let nextPieces =
                                    match piece with
                                    | Some i -> Map.remove i remainingPieces
                                    | None -> remainingPieces

                                nextPieces, completed

                            | PieceReceived(index, data) ->
                                let offset = int64 index * state.PieceSize

                                state.Stream.Seek(offset, SeekOrigin.Begin) |> ignore
                                state.Stream.Write(data, 0, data.Length)

                                printfn "Completed: %i of %i" (completed + 1) state.Pieces.Length

                                remainingPieces, completed + 1
                            | PieceFailed piece -> Map.add piece.Index piece remainingPieces, completed


                        if completed = state.Pieces.Length then
                            stream.Flush()
                            stream.Close()
                        // cts.Cancel()
                        else
                            return! loop nextPieces completed
                    }

                loop pieceMap 0)

        member _.HasUsefulPieces(pieces: BitArray) =
            agent.PostAndAsyncReply(fun channel -> HasUsefulPieces(pieces, channel))

        member _.RequestPiece(bitfield: BitArray) =
            agent.PostAndAsyncReply(fun channel -> RequestPiece(bitfield, channel))

        member _.PieceReceived(index: int, data: byte array) = agent.Post(PieceReceived(index, data))

        member _.PieceFailed(piece: PieceWork) = agent.Post(PieceFailed piece)
