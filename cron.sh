# ADD THIS TO YOUR CRONTAB BY RUNNING crontab cron.sh
DIRPATH='/Users/prabhas/Code/nigeria-analysis'
* * * * * cd $DIRPATH && python benchmark.py 10 1 10 100 1000 10000
