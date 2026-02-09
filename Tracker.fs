namespace BitTorrent.Client

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
    let private parser = BencodeParser()

    let private buildTrackerUrl (baseUrl: string) (parameters: TrackerParameters) =
        let query =
            [ "info_hash", Utils.encodeBytes parameters.InfoHash
              "peer_id", Utils.encodeBytes parameters.PeerId
              "port", string parameters.Port
              "uploaded", string parameters.Uploaded
              "downloaded", string parameters.Downloaded
              "left", string parameters.Left
              "compact", "1"
              "event", parameters.Event.ToString()
              "numwant", string 200 ]
            |> Seq.map (fun (k, v) -> sprintf "%s=%s" k v)
            |> String.concat "&"

        $"{baseUrl}?{query}"

    let private announceHttpTracker (baseUrl: string) (parameters: TrackerParameters) (client: HttpClient) =
        task {
            let url = buildTrackerUrl baseUrl parameters

            printfn "%s" url

            let! response = client.GetByteArrayAsync url

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

            let trackerResponse =
                { Interval = dictionary.Get<BNumber>("interval").Value |> int
                  Peers = peers }

            return trackerResponse
        }

    // NOTE: Create an abstraction for the client, maybe
    let announce (baseUrl: string) (parameters: TrackerParameters) (client: HttpClient) =
        task {
            // TODO: Implement announce to UDP tracker

            let! response = announceHttpTracker baseUrl parameters client

            printfn "Peers: %i" response.Peers.Length

            return response
        }
