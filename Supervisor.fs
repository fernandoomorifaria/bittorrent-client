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

module Supervisor =
    type Supervisor(state: State, cts: CancellationTokenSource) =
        let pieceMap = state.Pieces |> Array.map (fun p -> p.Index, p) |> Map.ofArray

        let agent =
            MailboxProcessor<CoordinatorMessage>.Start(fun inbox ->
                let rec loop (remainingPieces: Map<int, PieceWork>) (completedPieces: Set<int>) =
                    async {
                        let! message = inbox.Receive()

                        let stream = state.Stream

                        let nextPieces, nextCompleted =
                            match message with
                            | HasUsefulPieces(pieces, replyChannel) ->
                                pieceMap
                                |> Map.exists (fun index _ -> pieces.[index] && not (completedPieces.Contains index))
                                |> replyChannel.Reply

                                remainingPieces, completedPieces

                            | RequestPiece(bitfield, replyChannel) ->
                                let piece = remainingPieces |> Map.tryFindKey (fun index _ -> bitfield.[index])

                                match piece with
                                | Some i ->
                                    replyChannel.Reply(Some remainingPieces.[i])

                                    printfn "Assigning piece %i (%i remaining)" i (remainingPieces.Count - 1)

                                    Map.remove i remainingPieces, completedPieces
                                | None ->
                                    let inFlightPiece =
                                        pieceMap
                                        |> Map.tryFindKey (fun index _ ->
                                            bitfield.[index]
                                            && not (completedPieces.Contains index)
                                            && not (remainingPieces.ContainsKey index))

                                    match inFlightPiece with
                                    | Some i ->
                                        replyChannel.Reply(Some pieceMap.[i])

                                        printfn "Re-assigning in-flight piece %i" i
                                    | None ->
                                        replyChannel.Reply(None)

                                        printfn
                                            "No piece available for peer (%i remaining in map)"
                                            remainingPieces.Count

                                    remainingPieces, completedPieces

                            | PieceReceived(index, data) ->
                                if not (completedPieces.Contains index) then
                                    let offset = int64 index * state.PieceSize

                                    state.Stream.Seek(offset, SeekOrigin.Begin) |> ignore
                                    state.Stream.Write(data, 0, data.Length)

                                    let newCompleted = completedPieces.Add index

                                    printfn "Completed: %i of %i" newCompleted.Count state.Pieces.Length

                                    remainingPieces |> Map.remove index, newCompleted
                                else
                                    remainingPieces, completedPieces

                            | PieceFailed piece ->
                                if not (completedPieces.Contains piece.Index) then
                                    printfn
                                        "Piece %i returned to queue (%i remaining)"
                                        piece.Index
                                        (remainingPieces.Count + 1)

                                    Map.add piece.Index piece remainingPieces, completedPieces
                                else
                                    remainingPieces, completedPieces

                        if nextCompleted.Count = state.Pieces.Length then
                            stream.Flush()
                            stream.Close()
                            cts.Cancel()

                            // TODO: Check if it will be better if I end the loop
                            return! loop nextPieces nextCompleted
                        else
                            return! loop nextPieces nextCompleted
                    }

                loop pieceMap Set.empty)

        member _.HasUsefulPieces(pieces: BitArray) =
            agent.PostAndReply(fun channel -> HasUsefulPieces(pieces, channel))

        member _.RequestPiece(bitfield: BitArray) =
            agent.PostAndReply(fun channel -> RequestPiece(bitfield, channel))

        member _.PieceReceived(index: int, data: byte array) = agent.Post(PieceReceived(index, data))

        member _.PieceFailed(piece: PieceWork) = agent.Post(PieceFailed piece)
