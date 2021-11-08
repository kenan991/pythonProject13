import simpy
import pandas as pd


def clean_data(file_path):  # cleaning the workload data

    with open(file_path, "r") as csvfile:
        df_t = pd.read_csv(csvfile, index_col=0, parse_dates=True, dtype='unicode')

    df_t = df_t.reset_index(drop=True)
    df_t = df_t.drop(
        ['passenger_count', 'RatecodeID', 'store_and_fwd_flag', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
         'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge'], axis=1)
    df_t['tpep_pickup_datetime'] = pd.to_datetime(df_t['tpep_pickup_datetime'])
    df_t = df_t[~(df_t['tpep_pickup_datetime'] < '2020-06-01 00:00:00')]
    df_t = df_t[~(df_t['tpep_pickup_datetime'] > '2020-07-01 00:00:00')]
    df_t["c"] = 1
    # group the pickup action every 15 minutes
    df_t = df_t.groupby(pd.Grouper(freq='15min', key='tpep_pickup_datetime')).sum()["c"]
    df_t = df_t.to_frame()

    return df_t


class Battery:  # check the power drwan/feeded to the grid

    def __init__(self, capacity, charge_level=0):
        self.capacity = capacity
        self.charge_level = charge_level

    def update(self, energy):
        new_charge_level = self.charge_level + energy

        excess_energy = 0
        if new_charge_level < 0:
            excess_energy = self.charge_level
            new_charge_level = 0
        elif new_charge_level > self.capacity:
            excess_energy = self.charge_level - self.capacity
            new_charge_level = self.capacity

        self.charge_level = new_charge_level
        return excess_energy


DELTA_ENERGY = []
GRID_ENEERGY = []
CHARGE_LEVEL = []


def simulate(env, battery, production_df, consumption_df):
    for i in range(0, len(production_df)):
        delta_energy = production_df.iloc[i, 0] - consumption_df.iloc[i, 0]
        grid_energy = battery.update(delta_energy)

        DELTA_ENERGY.append(delta_energy)
        GRID_ENEERGY.append(grid_energy)
        CHARGE_LEVEL.append(battery.charge_level)
        yield env.timeout(1)


def dataframe(df_w, delta_energy, grid_energy, charge_level):
    data = {'DELTA_ENERGY': delta_energy, 'GRID_ENERGY': grid_energy, 'CHARGE_LEVEL': charge_level}
    dataframe = pd.DataFrame(data, index=df_w.index)
    return dataframe


def main(solar_area, load_factor, capacity):
    # reading the Weather data and drop the index and the unneeded columns

    solar_efficiency = 0.18

    with open("/Users/kenanskon/Documents/wetter.htw-berlin.de Values EGH_SMP4 from 2020-06-01 to 2020-07-01.txt", "r") as csvfile:
        df_w = pd.read_csv(csvfile, index_col=0, parse_dates=True)  # W/m^2
    production_df = df_w * solar_area * solar_efficiency  # W

    # reading the Taxi data and drop the index and the unneeded columns
    df_t = clean_data("/Users/kenanskon/Documents/Taxi/yellow_tripdata_2020-06.csv")
    print(df_t)

    consumption_df = df_t * load_factor

    battery = Battery(capacity=capacity)
    env = simpy.Environment()
    env.process(simulate(env, battery, production_df, consumption_df))
    env.run()

    result = dataframe(df_w, DELTA_ENERGY, GRID_ENEERGY, CHARGE_LEVEL)
    with open("result.csv", "w") as f:
        f.write(result.to_csv())
    print(result)
    return result


if __name__ == '__main__':
    main(5, 1.05, 15000)
