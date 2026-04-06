#!/usr/bin/env bash
# Turn all txt files in dbt/seeds/onet to csv
for file in *.txt; do
  sed 's/\t/,/g' "$file" >"${file%.txt}.csv"
done
