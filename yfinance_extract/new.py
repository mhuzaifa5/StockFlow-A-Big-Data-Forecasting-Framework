with open("store_previous_date.txt", "r") as file:
    start_date = file.readline().strip()
    
print(f"start_date: {start_date}")

