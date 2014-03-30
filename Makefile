
CXX=mpic++

run:: rwbw
	srun -A pal -p slurm --task-prolog=./prolog.sh --nodes=32 --ntasks-per-node=1 --label -- ./$<

