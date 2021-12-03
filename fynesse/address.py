import numpy as np
import statsmodels.api as sm

from fynesse import assess


def predict_price(database, latitude, longitude, date, property_type, region=0.02):
    """Price prediction for UK housing. Uses a feature matrix comprised of the 'closenesses' of all of the houses to each
    POI in the region (whether the house is within 0.005 latitude/longitude to the POI), along with the types of the
    houses."""

    # Get sales data
    region = (latitude, longitude, region)
    start_date = f"{date.year - 2}-{date.month}-{date.day}"
    end_date = f"{date.year + 2}-{date.month}-{date.day}"
    prices = database.get_prices_in_region(*region, start_date=start_date, end_date=end_date, limit=100000)

    num_relevant_houses = len(prices[prices.property_type == property_type])
    if num_relevant_houses == 0:
        print("No relevant houses!")
        return
    if num_relevant_houses < 5:
        print("Very few relevant houses! Quality of model may be poor")

    # Get pois
    centroids = assess.get_pois_centroids(region)

    def get_features(latitudes, longitudes, property_types):
        # Get distances to every poi
        closenesses = assess.get_clossness_matrix(centroids, latitudes, longitudes, cutoff=0.005)

        # Get property types
        types = np.concatenate(
            [np.array(np.array(property_types) == v).astype(int).reshape(-1, 1) for v in ["F", "S", "D", "T", "O"]],
            axis=1)

        return np.concatenate((closenesses, types, np.ones(len(latitudes)).reshape(-1, 1)), axis=1)

    # Train linear model
    design = get_features(prices.lattitude, prices.longitude, prices.property_type)
    m_linear_basis = sm.OLS(np.log(prices.price), design)
    results_basis = m_linear_basis.fit()

    if results_basis.llf / len(prices) < -1:
        print(results_basis.llf / len(prices))
        print("Model may not be accurate")

    # Insert our data to get a prediction
    pred_data = get_features([latitude], [longitude], [property_type])
    prediction = results_basis.get_prediction(pred_data)

    return int(np.exp(prediction.predicted_mean[0]))
