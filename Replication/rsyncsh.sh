#!/bin/bash

# set backup path
src=corex

# run backup
mkdir -p $src-backup

mydate="`date +%Y%m%d_%H%M%S`"
dest=$src-backup/$src-${mydate}

counter=0
while :
do
counter=$(($counter+1))
echo "$counter : rsync -iavzP $src $dest"
tmpfile=$(mktemp)
rsync -iavzP $src $dest  | grep '^>' > "$tmpfile"

if [[ $(wc -l < "$tmpfile") == "0" ]]; then
   break
else
  cat $tmpfile
fi

rm $tmpfile
sleep 2s
done

echo ""
echo "BACKUP: $dest"
