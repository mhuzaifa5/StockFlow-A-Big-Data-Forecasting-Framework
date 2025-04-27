import yfinance as yf
import pandas as pd
import datetime as dt

def get_data(start_date, end_date, interval):
    tickers = ["BRK-B"]  
    
    # Download stock data
    data = yf.download(tickers, start=start_date, end=end_date, interval=interval)
    
    # Save the data to a CSV file
    data.to_csv("stock_data.csv")
    
    print("Stock data downloaded and saved to stock_data.csv")
    return data.tail(1)

# Example usage
with open("last_date.txt", "r") as file:
    previous_date = file.readline().strip()


start_date = pd.to_datetime(previous_date)
# start_date = pd.to_datetime('2025-03-15 00:00:00')  # Replace with your desired start date
end_date = dt.datetime.now().strftime("%Y-%m-%d")  # Current date

result = get_data(start_date, end_date, "1d") 


# # Extract the datetime of the last row
last_datetime = result.reset_index()['Date'].iloc[-1]

# # Save the extracted datetime to the file
with open("last_date.txt", "w") as file:
    file.write(str(last_datetime.strftime("%Y-%m-%d")))


