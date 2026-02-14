namespace BitTorrent.Client

module Utils =
    let encodeBytes (bytes: byte[]) =
        bytes |> Array.map (fun b -> sprintf "%%%02x" b) |> String.concat ""
