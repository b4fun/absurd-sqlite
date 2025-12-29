input="$1"
if [ -z "$input" ]; then
  echo "Usage: $0 <input-image>"
  exit 1
fi

file_name_noext=$(basename "$input" | sed 's/\.[^.]*$//')

mkdir -p "${file_name_noext}"

for s in 16 24 32 48 64 96 128 256 512 1024; do
  magick "$input" -resize ${s}x${s} ${file_name_noext}/logo-${s}x${s}.png
done

magick "$input" -resize 256x256 \
  -define icon:auto-resize=16,24,32,48,64,128,256 \
  ${file_name_noext}/icon.ico

magick "$input" -resize 1024x1024 \
  ${file_name_noext}/icon.icns
