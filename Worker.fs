namespace BitTorrent.Client

open System.Collections
open System.Security.Cryptography
open Message
open Supervisor

type PieceProgress =
    { Piece: PieceWork
      Data: byte array
      Received: int
      Requested: int
      Requests: int }

type PieceResult =
    | RequestBlock of PieceProgress
    | PieceCompleted of byte array
    | PieceFailed

module Worker =
    let createPieceProgress (pieceWork: PieceWork) =
        { Piece = pieceWork
          Data = Array.zeroCreate<byte> pieceWork.Length
          Requested = 0
          Received = 0
          Requests = 0 }

    let checkIntegrity (hash: byte array) (piece: byte array) = SHA1.HashData piece = hash

    let sendRequest (sendMessage: MessageSender) (pieceProgress: PieceProgress) =
        task {
            let piece = pieceProgress.Piece
            let mutable progress = pieceProgress

            // TODO: Check if peer is unchoked
            while progress.Requests < MaxRequests && progress.Requests < piece.Length do
                let blockSize = min BlockSize (piece.Length - progress.Requested)

                let request = Request(piece.Index, progress.Requested, blockSize)

                // NOTE: Not proud of this but TCO doesn't work with Tasks
                do! sendMessage request

                progress <-
                    { progress with
                        Requests = progress.Requests + 1
                        Requested = progress.Requested + blockSize }

            return progress
        }

    let handleChoke (peer: PeerConnection) (supervisor: Supervisor) (pieceProgress: PieceProgress option) =
        match pieceProgress with
        | Some { Piece = piece } -> supervisor.PieceFailed piece
        | None -> ()

        { peer with AmChoking = true }

    // TODO: Maybe inject sendRequest instead of sendMessage
    let handleUnchoke (pieceWork: PieceWork) (sendMessage: MessageSender) (peer: PeerConnection) =
        task {
            let unchokedPeer = { peer with AmChoking = false }

            let pieceProgress = createPieceProgress pieceWork

            let! requestProgress = sendRequest sendMessage pieceProgress

            return unchokedPeer, Some requestProgress
        }

    let handleBitfield
        (bitfield: BitArray)
        (hasUsefulPieces: bool)
        (sendMessage: MessageSender)
        (peer: PeerConnection)
        =
        task {
            let withBitfield = { peer with Bitfield = Some bitfield }

            if hasUsefulPieces then
                do! sendMessage Interested

                return
                    { withBitfield with
                        AmInterested = true }
            else
                return
                    { withBitfield with
                        AmInterested = false }
        }

    let processPiece (pieceProgress: PieceProgress) (piece: Piece) =
        let pieceReceivedProgress =
            { pieceProgress with
                Received = pieceProgress.Received + piece.Block.Length
                Requests = pieceProgress.Requests - 1 }

        Array.blit piece.Block 0 pieceReceivedProgress.Data piece.Offset piece.Block.Length

        if pieceReceivedProgress.Received = pieceProgress.Piece.Length then
            if checkIntegrity pieceReceivedProgress.Piece.Hash pieceReceivedProgress.Data then
                PieceCompleted pieceReceivedProgress.Data
            else
                PieceFailed
        else
            RequestBlock pieceReceivedProgress

    let requestPiece (pieceWork: PieceWork) (sendMessage: MessageSender) =
        task {
            let progress = createPieceProgress pieceWork

            return! sendRequest sendMessage progress
        }

    type Worker(peer: PeerConnection, sendMessage: MessageSender, supervisor: Supervisor) =
        let agent =
            MailboxProcessor<MessageResult>.Start(fun inbox ->
                let rec loop (peer: PeerConnection) (pieceProgress: PieceProgress option) =
                    async {
                        let! message = inbox.Receive()

                        match message with
                        | Message message ->
                            match message with
                            | Interested
                            | NotInterested
                            | Request _
                            | Cancel _
                            | KeepAlive
                            | Have _ -> return! loop peer pieceProgress
                            | Choke ->
                                let chokedPeer = handleChoke peer supervisor pieceProgress

                                return! loop chokedPeer None
                            | Unchoke ->
                                let! pieceWork = supervisor.RequestPiece peer.Bitfield.Value

                                match pieceWork with
                                | None -> ()
                                | Some work ->
                                    let! unchokedPeer, pieceProgress =
                                        handleUnchoke work sendMessage peer |> Async.AwaitTask

                                    return! loop unchokedPeer pieceProgress
                            | Bitfield bitfield ->
                                let! hasUsefulPieces = supervisor.HasUsefulPieces bitfield

                                let! peerWithBitfield =
                                    handleBitfield bitfield hasUsefulPieces sendMessage peer |> Async.AwaitTask

                                return! loop peerWithBitfield pieceProgress
                            | Piece piece ->
                                let result = processPiece pieceProgress.Value piece

                                match result with
                                | RequestBlock pieceProgress ->
                                    let! nextRequestProgress = sendRequest sendMessage pieceProgress |> Async.AwaitTask

                                    return! loop peer (Some nextRequestProgress)
                                | PieceCompleted data ->
                                    supervisor.PieceReceived(piece.Index, data)

                                    do! sendMessage (Have piece.Index)

                                    let! pieceWork = supervisor.RequestPiece peer.Bitfield.Value

                                    match pieceWork with
                                    | None -> ()
                                    | Some work ->
                                        let! nextPieceProgress = requestPiece work sendMessage |> Async.AwaitTask

                                        return! loop peer (Some nextPieceProgress)
                                | PieceFailed ->
                                    supervisor.PieceFailed pieceProgress.Value.Piece

                                    let! pieceWork = supervisor.RequestPiece peer.Bitfield.Value

                                    match pieceWork with
                                    | None -> ()
                                    | Some work ->
                                        let! nextPieceProgress = requestPiece work sendMessage |> Async.AwaitTask

                                        return! loop peer (Some nextPieceProgress)
                        | PeerDisconnected reason ->
                            printfn "Peer disconnected: %s" reason

                            match pieceProgress with
                            | Some { Piece = piece } -> supervisor.PieceFailed piece
                            | None -> ()

                            peer.Connection.Close()
                    }

                loop peer None)

        member _.ProcessMessage(message: MessageResult) = agent.Post message

    let rec startWorker (readMessage: MessageReader) (worker: Worker) =
        async {
            let! message = readMessage () |> Async.AwaitTask

            worker.ProcessMessage message

            match message with
            | PeerDisconnected _ -> ()
            | Message _ -> return! startWorker readMessage worker
        }
