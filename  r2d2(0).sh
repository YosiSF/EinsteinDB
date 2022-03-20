#!/bin/bash
for i in *; do  # run `ls -1` and iterate over the result.
  j=0   # will be used to ensure uniqueness of file names.

  while [ -f "${i%.*}($j).${i#*.}" ]; do
    (( j++ ))       # increment until we get a unique file name.
  done

  mv -- "$i" " ${i%.*}($j).${i#*.}"   # move (rename) the current file to a new name, like "file.txt" becomes "file_0.txt". If you're sure there are no collisions, then remove this line and unindent the next one below it so that everything is indented by 1 tab character (= 4 spaces). Otherwise, leave everything as-is if you want to rename files only if needed to avoid overwriting anything existing which has not yet been renamed by this script yet. This prevents doing unnecessary work and protects any existing files from being wrongly overwritten!
done