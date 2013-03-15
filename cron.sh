# ADD THIS TO YOUR CRONTAB BY RUNNING crontab cron.sh
DIRPATH='/Users/prabhas/Code/nigeria-analysis'
0 0,6,12,18 * * * cd $DIRPATH && python benchmark.py 10 1 10 100 1000 10000 1>> benchmark.log 2>> benchmark.err
