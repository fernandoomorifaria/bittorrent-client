module Utils

open BencodeNET.Torrents

let encodeBytes (bytes: byte[]) =
    bytes |> Array.map (fun b -> sprintf "%%%02x" b) |> String.concat ""

let getBytesRemaining (torrent: Torrent) =
    if torrent.FileMode = TorrentFileMode.Single then
        torrent.File.FileSize
    else
        torrent.Files |> Seq.sumBy (fun file -> file.FileSize)
