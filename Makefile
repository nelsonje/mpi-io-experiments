
CXX=mpic++
NODES=32
PPN=1

run:: rwbw
	srun -A pal -p pal --task-prolog=./prolog.sh --nodes=$(NODES) --ntasks-per-node=$(PPN) --label -- ./$<

