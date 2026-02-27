namespace BitTorrent.Client

open Message
open Coordinator

type PieceProgress =
    { Piece: PieceWork
      Received: int
      Requested: int
      Requests: int }

module Worker =
    open System.Net.Sockets
    open System.Threading.Tasks

    let createPieceProgress (pieceWork: PieceWork) =
        { Piece = pieceWork
          Requested = 0
          Received = 0
          Requests = 0 }

    let sendRequest (connection: PeerConnection) (pieceProgress: PieceProgress) =
        task {
            let stream = connection.Connection.GetStream()

            let rec pipeline (stream: NetworkStream) (pieceProgress: PieceProgress) : Task<PieceProgress> =
                task {
                    let piece = pieceProgress.Piece

                    if pieceProgress.Requests < MaxRequests && pieceProgress.Received < piece.Length then
                        let blockSize = min BlockSize (piece.Length - pieceProgress.Requested)

                        let request = Request(piece.Index, pieceProgress.Requested, blockSize)

                        let! _ = stream.WriteAsync(serialize request)

                        let updatedProgress =
                            { pieceProgress with
                                Requests = pieceProgress.Requests + 1
                                Requested = pieceProgress.Requested + blockSize }

                        return! pipeline stream updatedProgress
                    else
                        return pieceProgress
                }

            return! pipeline stream pieceProgress
        }

    type Worker(connection: PeerConnection, coordinator: Coordinator) =
        let agent =
            MailboxProcessor<Message>.Start(fun inbox ->
                let rec loop (connection: PeerConnection) (pieceProgress: PieceProgress option) =
                    async {
                        let! message = inbox.Receive()

                        match message with
                        | Unchoke ->
                            let unchoked = { connection with AmChoking = false }

                            let! pieceWork = coordinator.RequestPiece connection.Bitfield.Value

                            match pieceWork with
                            | None -> return! loop unchoked pieceProgress
                            | Some pieceWork ->
                                let progress =
                                    pieceProgress |> Option.defaultWith (fun () -> createPieceProgress pieceWork)

                                let! updatedProgress = sendRequest connection progress |> Async.AwaitTask

                                return! loop unchoked (Some updatedProgress)
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
                        | Piece(_, _, block) ->
                            let pieceProgress = pieceProgress.Value

                            let updatedProgress =
                                { pieceProgress with
                                    Requests = pieceProgress.Requested - 1
                                    Requested = pieceProgress.Received + block.Length }

                            if updatedProgress.Requests < MaxRequests then
                                let! a = sendRequest connection updatedProgress |> Async.AwaitTask

                                ()
                            else
                                ()

                        ()
                    }

                loop connection None)

        member _.ProcessMessage(message: Message) = agent.Post message
