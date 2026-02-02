open System.Text
open System.Linq
open System.Net
open System.Net.Http
open BencodeNET.Parsing
open BencodeNET.Objects
open BencodeNET.Torrents

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

type Peer = { Ip: IPAddress; Port: uint16 }

type TrackerResponse =
    { // FailureReason: string option
      Interval: int
      Peers: Peer array }

let encodeBytes (bytes: byte[]) =
    bytes |> Array.map (fun b -> sprintf "%%%02x" b) |> String.concat ""

let buildTrackerUrl (baseUrl: string) (parameters: TrackerParameters) =
    let query =
        [ "info_hash", encodeBytes parameters.InfoHash
          "peer_id", encodeBytes parameters.PeerId
          "port", string parameters.Port
          "uploaded", string parameters.Uploaded
          "downloaded", string parameters.Downloaded
          "left", string parameters.Left
          "compact", "1"
          "event", parameters.Event.ToString() ]
        |> Seq.map (fun (k, v) -> sprintf "%s=%s" k v)
        |> String.concat "&"

    $"{baseUrl}?{query}"

let client = new HttpClient()

let parser = BencodeParser()

let torrent =
    parser.Parse<Torrent> "./kali-linux-2025.4-installer-amd64.iso.torrent"

let announceHttpTracker (baseUrl: string) (parameters: TrackerParameters) =
    task {
        let url = buildTrackerUrl baseUrl parameters

        printfn "%s" url

        let! response = client.GetByteArrayAsync url

        let dictionary = parser.Parse<BDictionary> response

        let peersBytes = dictionary.Get<BString>("peers").EncodeAsBytes()

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

let announce (baseUrl: string) (parameters: TrackerParameters) =
    task {
        let! response = announceHttpTracker baseUrl parameters

        printfn "%A" response

        ()
    }

let left (torrent: Torrent) =
    if torrent.FileMode = TorrentFileMode.Single then
        torrent.File.FileSize
    else
        torrent.Files |> Seq.sumBy (fun file -> file.FileSize)

let peerId = "-qB4500-abc123def456"

// TODO: Iterate other trackers
let baseUrl = torrent.Trackers.First().First()

let request =
    { InfoHash = torrent.OriginalInfoHashBytes
      PeerId = Encoding.ASCII.GetBytes peerId
      Port = 58237
      Uploaded = 0
      Downloaded = 0
      Left = left torrent
      Event = Started }

// TODO: Announce for each tracker
announce baseUrl request |> Async.AwaitTask |> Async.RunSynchronously
