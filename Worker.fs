namespace BitTorrent.Client

open System.Net.Sockets
open System.Threading.Tasks
open System.Security.Cryptography
open Message
open Coordinator

type PieceProgress =
    { Piece: PieceWork
      Data: byte array
      Received: int
      Requested: int
      Requests: int }

module Worker =
    let createPieceProgress (pieceWork: PieceWork) =
        { Piece = pieceWork
          Data = Array.zeroCreate<byte> pieceWork.Length
          Requested = 0
          Received = 0
          Requests = 0 }

    let checkIntegrity (hash: byte array) (piece: byte array) =
        let pieceHash = SHA1.HashData piece

        hash = pieceHash

    let rec pipeline (stream: NetworkStream) (pieceProgress: PieceProgress) =
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

    // TODO: Use cancellation token with timeout
    let sendRequest (connection: PeerConnection) (pieceProgress: PieceProgress) =
        task {
            let stream = connection.Connection.GetStream()

            return! pipeline stream pieceProgress
        }

    type Worker(connection: PeerConnection, coordinator: Coordinator) =
        let agent =
            MailboxProcessor<Message>.Start(fun inbox ->
                let rec loop (connection: PeerConnection) (pieceProgress: PieceProgress option) =
                    async {
                        try
                            let! message = inbox.Receive()

                            match message with
                            | Interested
                            | NotInterested
                            | Have _
                            | Request _
                            | Cancel _
                            | KeepAlive -> return! loop connection pieceProgress
                            | Choke -> return! loop { connection with AmChoking = true } pieceProgress
                            | Unchoke ->
                                let unchoked = { connection with AmChoking = false }

                                let! pieceWork = coordinator.RequestPiece connection.Bitfield.Value

                                match pieceWork with
                                | None -> return! loop unchoked pieceProgress
                                | Some pieceWork ->
                                    let progress = createPieceProgress pieceWork

                                    let! updatedProgress = sendRequest connection progress |> Async.AwaitTask

                                    return! loop unchoked (Some updatedProgress)
                            | Bitfield bitfield ->
                                let withBitfield =
                                    { connection with
                                        Bitfield = Some bitfield }

                                let! hasUsefulPieces = coordinator.HasUsefulPieces bitfield

                                if hasUsefulPieces then
                                    // TODO: Move this somewhere else
                                    let stream = connection.Connection.GetStream()

                                    do! stream.AsyncWrite(serialize Interested)

                                    return!
                                        loop
                                            { withBitfield with
                                                AmInterested = true }
                                            pieceProgress
                                else
                                    return! loop withBitfield pieceProgress
                            | Piece(_, offset, block) ->
                                let pieceProgress = pieceProgress.Value
                                let piece = pieceProgress.Piece

                                System.Array.Copy(block, 0, pieceProgress.Data, offset, block.Length)

                                let updatedProgress =
                                    { pieceProgress with
                                        Received = pieceProgress.Received + block.Length
                                        Requests = pieceProgress.Requests - 1 }

                                if updatedProgress.Received < piece.Length then
                                    if
                                        updatedProgress.Requests < MaxRequests
                                        && updatedProgress.Requested < piece.Length
                                    then
                                        let! nextRequest = sendRequest connection updatedProgress |> Async.AwaitTask
                                        return! loop connection (Some nextRequest)
                                    else
                                        return! loop connection (Some updatedProgress)
                                else if checkIntegrity piece.Hash updatedProgress.Data then
                                    coordinator.PieceReceived(piece.Index, updatedProgress.Data)

                                    let! nextPiece = coordinator.RequestPiece connection.Bitfield.Value

                                    match nextPiece with
                                    | Some work ->
                                        let newProgress = createPieceProgress work

                                        let! firstRequest = sendRequest connection newProgress |> Async.AwaitTask
                                        return! loop connection (Some firstRequest)
                                    | None -> return! loop connection None
                                else
                                    printfn "Piece %i failed" piece.Index

                                    coordinator.PieceFailed pieceProgress.Piece

                                    let! nextPiece = coordinator.RequestPiece connection.Bitfield.Value

                                    match nextPiece with
                                    | Some work ->
                                        let newProgress = createPieceProgress work
                                        let! firstRequest = sendRequest connection newProgress |> Async.AwaitTask
                                        return! loop connection (Some firstRequest)
                                    | None -> return! loop connection None
                        with _ ->
                            match pieceProgress with
                            | Some progress -> coordinator.PieceFailed progress.Piece
                            | None -> ()

                            return! loop connection None
                    }

                loop connection None)

        member _.ProcessMessage(message: Message) = agent.Post message
