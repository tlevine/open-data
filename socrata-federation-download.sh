#!/bin/sh
set -e

d=$(date --rfc-3339 date)
dir=downloads/socrata-homepages/$d
mkdir -p $dir
(
  cd downloads/socrata-homepages
  rm -f current
  ln -s $d current
)

for portal in $(ls downloads/socrata); do
    file="${dir}/${portal}.html"
    test -e $file || wget --no-check-certificate -O "$file" "https://${portal}/browse/embed"
done

# Skip opendata.socrata.com
