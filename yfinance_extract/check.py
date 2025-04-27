import datetime 

with open("last_date.txt", "w") as file:
    file.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))