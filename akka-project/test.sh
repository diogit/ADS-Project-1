start "[MAIN TESTER]" ".\target\pack\bin\main" 10.22.110.98 230 10.22.110.98 230
for port in {231..249}
do
	start " " ".\target\pack\bin\main" 10.22.110.98 230 10.22.110.98 $port 10.22.110.98 $((port - 1))
	sleep 2 # Wait before launching another node
done
