x=$1
if [ $x -ne 0 ]; then
    ps -u `id -un` | grep "python" | awk '{ print $1}'
else
    ps -u `id -un` | grep "Python" | awk '{ print $2}'
fi
