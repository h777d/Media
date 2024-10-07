import os

Plot_path = '/Users/hosseind/Downloads/case/sales_forecast.png'
model_path = '/Users/hosseind/Downloads/case/arima_model.pkl'
save_model = True

# ARIMA Model Configuration
ARIMA_ORDER = (2, 1, 0)  # p, d, q parameters
FORECAST_STEPS = 12  # months