namespace BitTorrent.Client

open System
open System.Threading.Tasks
open System.Net
open System.Net.Http
open BencodeNET.Parsing
open BencodeNET.Objects

type Event =
    | Started
    | Stopped
    | Completed

    override this.ToString() =
        match this with
        | Started -> "started"
        | Stopped -> "stopped"
        | Completed -> "completed"

type TrackerParameters =
    { InfoHash: byte array
      PeerId: byte array
      Port: int
      Uploaded: int
      Downloaded: int
      Left: int64
      Event: Event }

type TrackerResponse =
    { // FailureReason: string option
      Interval: int
      Peers: Peer array }

module Tracker =
    let parseTrackerResponse (response: byte array) =
        let parser = BencodeParser()

        let dictionary = parser.Parse<BDictionary> response

        let peersBytes = dictionary.Get<BString>("peers").Value.ToArray()

        let peers =
            peersBytes
            |> Array.chunkBySize 6
            |> Array.filter (fun chunk -> chunk.Length = 6)
            |> Array.map (fun chunk ->
                let ip = IPAddress(chunk[0..3])
                let port = uint16 chunk[4] <<< 8 ||| uint16 chunk[5]

                { Ip = ip; Port = port })
            |> Array.filter (fun peer -> not (IPAddress.IsLoopback peer.Ip))

        let trackerResponse =
            { Interval = dictionary.Get<BNumber>("interval").Value |> int
              Peers = peers }

        trackerResponse

    let buildHttpTrackerUrl (url: Uri) (parameters: TrackerParameters) =
        let query =
            [ "info_hash", Utils.encodeBytes parameters.InfoHash
              "peer_id", Utils.encodeBytes parameters.PeerId
              "port", string parameters.Port
              "uploaded", string parameters.Uploaded
              "downloaded", string parameters.Downloaded
              "left", string parameters.Left
              "compact", "1"
              "event", parameters.Event.ToString() ]
            |> Seq.map (fun (k, v) -> sprintf "%s=%s" k v)
            |> String.concat "&"

        $"{url}?{query}"

    let announceHttpTracker (url: Uri) (parameters: TrackerParameters) (client: HttpClient) =
        task {
            let url = buildHttpTrackerUrl url parameters

            printfn "Announcing to Http tracker: %s" (url.ToString())

            let! response = client.GetByteArrayAsync url

            return parseTrackerResponse response
        }

    let announce (urls: Uri array) (parameters: TrackerParameters) (client: HttpClient) =
        urls
        |> Array.map (fun url -> announceHttpTracker url parameters client)
        |> Task.WhenAll
