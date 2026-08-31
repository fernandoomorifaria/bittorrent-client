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

type SupervisorMessage =
    | HasUsefulPieces of pieces: BitArray * replyChannel: AsyncReplyChannel<bool>
    | RequestPiece of bitfield: BitArray * replyChannel: AsyncReplyChannel<PieceWork option>
    | PieceReceived of index: int * data: byte array
    | PieceFailed of PieceWork

module Supervisor =
    let writePieceToDisk (stream: FileStream) (index: int) (data: byte array) (pieceSize: int64) =
        task {
            let offset = int64 index * pieceSize

            stream.Seek(offset, SeekOrigin.Begin) |> ignore
            do! stream.WriteAsync(data, 0, data.Length)
        }

    type Supervisor(state: State, cts: CancellationTokenSource) =
        let pieceMap = state.Pieces |> Array.map (fun p -> p.Index, p) |> Map.ofArray

        let agent =
            MailboxProcessor<SupervisorMessage>.Start(fun inbox ->
                let rec loop (remainingPieces: Map<int, PieceWork>) (completedPieces: Set<int>) =
                    async {
                        let! message = inbox.Receive()

                        let stream = state.Stream

                        match message with
                        | HasUsefulPieces(pieces, replyChannel) ->
                            pieceMap
                            |> Map.exists (fun index _ -> pieces.[index] && not (completedPieces.Contains index))
                            |> replyChannel.Reply

                            return! loop remainingPieces completedPieces
                        | RequestPiece(bitfield, replyChannel) ->
                            let piece = remainingPieces |> Map.tryFindKey (fun index _ -> bitfield.[index])

                            match piece with
                            | Some i ->
                                replyChannel.Reply(Some remainingPieces.[i])

                                return! loop (Map.remove i remainingPieces) completedPieces
                            | None ->
                                replyChannel.Reply None

                                return! loop remainingPieces completedPieces
                        | PieceReceived(index, data) ->
                            if not (completedPieces.Contains index) then
                                do! writePieceToDisk state.Stream index data state.PieceSize |> Async.AwaitTask

                                let newCompleted = completedPieces.Add index
                                let nextPieces = remainingPieces |> Map.remove index

                                printfn "Completed: %i of %i" newCompleted.Count state.Pieces.Length

                                if newCompleted.Count = state.Pieces.Length then
                                    stream.Flush()
                                    stream.Close()
                                    cts.Cancel()

                                    printfn "Download finished."

                                    return! loop nextPieces newCompleted
                                else
                                    return! loop nextPieces newCompleted
                            else
                                return! loop remainingPieces completedPieces

                        | PieceFailed piece ->
                            if not (completedPieces.Contains piece.Index) then
                                return! loop (Map.add piece.Index piece remainingPieces) completedPieces
                            else
                                return! loop remainingPieces completedPieces
                    }

                loop pieceMap Set.empty)

        member _.HasUsefulPieces(pieces: BitArray) =
            agent.PostAndAsyncReply(fun channel -> HasUsefulPieces(pieces, channel))

        member _.RequestPiece(bitfield: BitArray) =
            agent.PostAndAsyncReply(fun channel -> RequestPiece(bitfield, channel))

        member _.PieceReceived(index: int, data: byte array) = agent.Post(PieceReceived(index, data))

        member _.PieceFailed(piece: PieceWork) = agent.Post(PieceFailed piece)
