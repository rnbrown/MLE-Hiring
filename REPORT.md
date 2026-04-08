# Assumptions and Tradeoffs
# Productionizing
    Ingestion:
        Monthly schedule (assuming we are receiving and predicting monthly aggregate data)
        Monitor number of rows populated by each script vs expected, and raise an alert if the percentage falls below some threshold

    The model:
        Monthly retraining
        The model will be trained on a sliding window that extends 6 months or some other reasonably long history
        Model training and testing metrics can be logged to the database, and alerts generated if the testing metric falls beyond a certain threshold
        We can set some retention bound for all retrieved data and trained models
    