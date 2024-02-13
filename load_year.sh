#!/bin/bash

# Usage: ./load_year.sh -s <idsite> -y <year>

while getopts "s:y:" opt; do
  case $opt in
    s)
      idsite=$OPTARG
      ;;
    y)
      year=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

if [[ -z $idsite || -z $year ]]; then
  echo "Usage: ./load_year.sh -s <idsite> -y <year>"
  exit 1
fi

for month in {1..12}; do
  python s3parket2elastic.py -s $idsite -y $year -m $month
done
