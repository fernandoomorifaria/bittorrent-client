namespace BitTorrent.Client

open BencodeNET.Torrents

module Utils =
    let encodeBytes (bytes: byte[]) =
        bytes |> Array.map (fun b -> sprintf "%%%02x" b) |> String.concat ""

// TODO: Create an util to convert to big-endian
