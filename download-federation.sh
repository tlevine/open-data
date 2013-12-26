#!/bin/sh
set -e

dir=downloads/socrata-homepages/$(date --rfc-3339 date)
mkdir -p $dir
rm -f downloads/socrata-homepages/current
ln -s $dir downloads/socrata-homepages/current

for portal in $(ls downloads/socrata); do
    file="downloads/socrata-homepages/${portal}.html"
    test -e $file || wget -O "$file" "https://${portal}/browse/embed"
done

# Skip opendata.socrata.com
