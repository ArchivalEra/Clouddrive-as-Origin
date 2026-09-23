#!/usr/bin/env bash
# Package an ordinary MP4 for a ready-made player, without re-encoding it.
#
# The product's object is one big progressive MP4, and hls.js/dash.js cannot do
# anything with that file as-is (they play segments, not files). What they CAN
# play is the same single file addressed by byte ranges: `#EXT-X-BYTERANGE`
# segments of one `.m4s`, which is exactly the shape this origin already serves
# well (206 per range, one upstream open per window). ffmpeg does the packaging
# as a stream copy — it rewrites the sample table, writes an init segment and
# N-second fragments into ONE file, and a VOD playlist pointing at byte ranges
# of it. No re-encode, no quality loss, and nothing media-aware comes near
# `src/`: the origin keeps serving a generic object, byte ranges included.
#
#   package-for-player.sh <in.mp4> <out-dir> [segment-seconds]
#
# Output: <out-dir>/hls.m4s (init + all fragments, one file) and hls.m3u8.
set -eu
IN=${1:?usage: package-for-player.sh <in.mp4> <out-dir> [segment-seconds]}
OUT=${2:?usage: package-for-player.sh <in.mp4> <out-dir> [segment-seconds]}
SEG=${3:-6}
mkdir -p "$OUT"
ffmpeg -y -loglevel error -i "$IN" -c copy \
  -f hls -hls_segment_type fmp4 -hls_flags single_file \
  -hls_time "$SEG" -hls_playlist_type vod "$OUT/hls.m3u8"
ls -l "$OUT"
echo "--- playlist head ---"
head -12 "$OUT/hls.m3u8"
