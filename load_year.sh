#!/bin/bash

# Usage: ./load_year.sh -s <idsite> -y <year>

while getopts "s:y:" opt; do
  case $opt in
#!/bin/bash

# Usage: ./load_year.sh -s <idsite> -y <year> -t <type>

type="R"

while getopts "s:y:t:" opt; do
  case $opt in
    s)
      idsite=$OPTARG
      ;;
    y)
      year=$OPTARG
      ;;
    t)
      type=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

if [[ -z $idsite || -z $year ]]; then
  echo "Usage: ./load_year.sh -s <idsite> -y <year> -t <type>"
  exit 1
fi

for month in {1..12}; do
  python3.10 s3parquet2elastic.py -s $idsite -y $year -m $month -t $type
done
