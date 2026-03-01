namespace BitTorrent.Client

open System
open System.Buffers.Binary
open System.Collections

module Utils =
    let encodeBytes (bytes: byte[]) =
        bytes |> Array.map (fun b -> sprintf "%%%02x" b) |> String.concat ""

    let writeBigEndian (value: int) =
        let bytes = Array.zeroCreate<byte> 4
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(), value)
        bytes

    let readBigEndian (bytes: byte array) (offset: int) =
        BinaryPrimitives.ReadInt32BigEndian(bytes.[offset .. offset + 3])

    let bitArrayToBytes (bitArray: BitArray) =
        let byteArray = Array.zeroCreate<byte> ((bitArray.Length + 7) / 8)
        bitArray.CopyTo(byteArray, 0)
        byteArray
