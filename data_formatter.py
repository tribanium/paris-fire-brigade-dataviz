from ast import parse
import pandas as pd
import random


"""
x_train :


|id      |name                                                |type        |comments|
|--------|----------------------------------------------------|------------|--------|
|#0      |emergency vehicle selection                     x    |int         |id de l'instance de sélection d'un véhicule d'urgence pour une intervention : 5271704|
|#1      |intervention                                    x    |int         |id de l'intervention : 13535032|
|#2      |alert reason category                               |int         |{1,...,9} : 3|
|#3      |alert reason                                    x    |int         |? : 2113|
|#4      |intervention on public roads                        |bool        |1 pour intervention sur voie publique / 0 sinon|
|#5      |floor                                               |int         |étage de l'intervention : 2|
|#6      |location of the event                           x    |float       |qualifie le lieu de l'urgence (hall d'entrée, autoroute...) : 136.0 // [float mais toujours avec des valeurs entières]|
|#7      |longitude intervention                              |float       |approximate longitude : 2.464084|
|#8      |latitude intervention                               |float       |approximate latitude : 48.818439|
|#9      |emergency vehicle                               x    |int         |id du véhicule de secours : 5755|
|#10     |emergency vehicle type                              |str         |type de véhicule de secours : VSAV BSPP|
|#11     |rescue center                                       |int         |id du centre de secours où appartient le véhicule d'intervention : 2483|
|#12     |selection time                                      |datetime    |selection time of the emergency vehicle : 2018-10-02 12:41:22.637|
|#13     |date key sélection                              x    |int         |selection date in YYYYMMDD format : 20181002|
|#14     |time key sélection                              x    |int         |selection time in HHMMSS format : 124122|
|#15     |status preceding selection                      x    |str         |statut du véhicule de secours avant sa sélection : disponible (pas à sa caserne) / rentré (garé à la caserne)|
|#16     |delta status preceding selection-selection      x    |int         |nombre de secondes entre la sélection du véhicule et l'update de son statut : 953|
|#17     |departed from its rescue center                 x    |bool        |1 si le véhicule est parti de la caserne / 0 sinon|
|#18     |longitude before departure                          |float       |longitude de la position du véhicule avant départ : 2.481148|
|#19     |latitude before departure                           |float       |latitude de la position du véhicule avant départ : 48.841034|
|#20     |delta position gps previous departure-departure x    |int         |number of seconds before the selection of the vehicle where its GPS position was recorded (when not parked at its emergency center)|
|#21     |GPS tracks departure-presentation               x    |?           |successive GPS positions|
|#22     |GPS tracks datetime departure-presentation      x    |?           |datetime list associated with successive GPS positions|
|#23     |OSRM response                                   x    |json        |service route response of an OSRM instance setup with IDF OpenStreetMap data|
|#24     |OSRM estimated distance                             |float       |distance calculated by the OSRM route service|
|#25     |OSRM estimated duration                             |float       |transit delay calculated bu the OSRM route service|

y_train :

|id     |name                                                |type        |comments|
|-------|----------------------------------------------------|------------|--------|
|1      |emergency vehicle selection                         |int         |id de l'instance de sélection d'un véhicule d'urgence pour une intervention : 5271704|
|2      |delta selection-departure                           |int         |secondes écoulées entre la sélection et le départ du véhicule d'intervention|
|3      |delta departure-presentation                        |int         |secondes écoulées entre le départ du véhicule d'intervention et l'arrivée sur le lieu|
|4      |delta selection-presentation                        |int         |secondes écoulées entre la sélection du véhicule d'urgence et l'arrivée sur le lieu d'intervention (somme des deux précédents)|

"""

x_path = "./dataset/x_train.csv"
y_path = "./dataset/y_train.csv"


def generate_csv(sample=False):
    df_merged = merge_data()
    if sample:
        out = df_merged.sample(40000, random_state=42)
        out.to_csv("sample.csv", index=False)
    else:
        df_merged.to_csv("data.csv", index=False)


def merge_data():
    columns_to_drop = [
        "emergency_vehicle_selection",
        "intervention",
        "alert_reason",
        "location_of_the_event",
        "emergency_vehicle",
        "date_key_sélection",
        "time_key_sélection",
        "status_preceding_selection",
        "delta_status_preceding_selection-selection",
        "delta_position_gps_previous_departure-departure",
        "GPS_tracks_departure-presentation",
        "GPS_tracks_datetime_departure-presentation",
        "OSRM_response",
    ]
    """Fusionne x et y et retourne le dataframe complet"""
    df1 = pd.read_csv(x_path, sep=",")
    new_columns_df1 = {old_col: old_col.replace(" ", "_") for old_col in df1.columns}
    df1.rename(new_columns_df1, axis=1, inplace=True)
    df2 = pd.read_csv(y_path, sep=",")
    new_columns_df2 = {
        old_col: f"{old_col.replace(' ', '_')}" for old_col in df2.columns
    }
    df2.rename(new_columns_df2, axis=1, inplace=True)

    df_merged = pd.merge(
        df1,
        df2,
        left_on="emergency_vehicle_selection",
        right_on="emergency_vehicle_selection",
        how="inner",
    )
    # print(df_merged.columns)
    df_merged.drop(columns_to_drop, axis=1, inplace=True)
    # print(df_merged.columns)
    return df_merged


def parse_rescue_centers():
    df_merged = merge_data()
    ret_median = (
        df_merged[df_merged["departed_from_its_rescue_center"] == True]
        .groupby(by="rescue_center")
        .median()[["longitude_before_departure", "latitude_before_departure"]]
    )

    ret_count = (
        df_merged[df_merged["departed_from_its_rescue_center"] == True]
        .groupby(by="rescue_center")
        .count()["selection_time"]
    )

    ret = (
        ret_median.merge(ret_count, left_index=True, right_index=True)
        .rename(columns={"selection_time": "count_interventions"})
        .reset_index()
    )

    ret.to_csv("data/casernes.csv", index=False)


if __name__ == "__main__":
    # generate_csv()
    parse_rescue_centers()
