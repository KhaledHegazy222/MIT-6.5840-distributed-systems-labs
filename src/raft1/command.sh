mkdir -p tmp
rm -rf tmp/*
rm -rf result.txt
for i in {1..1000}; do
  (
    outfile="tmp/run_${i}.log"
    if go test -race -v -run 3B >"$outfile" 2>&1; then
      echo "################# i=$i #################" >>result.txt
    else
      echo "Error occurred at i=$i, see $outfile" >>result.txt
      exit
    fi
  ) &
  # limit number of concurrent jobs (say 40 at a time)
  if (($(jobs -r -p | wc -l) >= 100)); then
    wait -n
  fi
  sleep 0.1
done
wait

echo "FINISHED YA KBEER"

cat tmp/* | grep -i -e "fatal" -e "/var" -e "panic" -e "gor"
cat result.txt | grep -i "run"
