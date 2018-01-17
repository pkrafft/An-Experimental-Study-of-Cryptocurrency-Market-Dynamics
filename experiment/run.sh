
# sh run.sh ../out/debug/ 5e-7 debug 
# sh run.sh ../out/epsilon-5en7-complete/ 5e-7 run

DIR=$1

if [ ! -d "$DIR" ]; then
    mkdir $DIR
fi

cp run.sh $DIR/
cp main.py $DIR/
cp experiment.py $DIR/
cp utils.py $DIR/
cp process.sh $DIR/
cp README $DIR/
echo 'sh run.sh '$1' '$2' '$3 > $DIR/command.sh 

python main.py $DIR $2 $3 >> $DIR/stdout 2>&1 &
