"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    import os, glob
    import pandas as pd

    df_clients = pd.DataFrame(
        columns=[
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
    )
    df_campaign = pd.DataFrame(
        columns=[
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "last_contact_date",
        ]
    )
    df_economics = pd.DataFrame(
        columns=[
            "client_id",
            "cons_price_idx",
            "euribor_three_months",
        ]
    )


    def proccess_client_data(data):
        aux_df = data.copy()
        aux_df["job"] = aux_df["job"].str.replace(".", "", regex=False)
        aux_df["job"] = aux_df["job"].str.replace("-", "_", regex=False)

        aux_df["education"] = aux_df["education"].str.replace(".", "_", regex=False)
        aux_df["education"] = aux_df["education"].replace("unknown", pd.NA)

        aux_df["credit_default"] = aux_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)

        aux_df["mortgage"] = aux_df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

        return aux_df

    def build_client_csv(base, data_frame):
        client_data = data_frame[
            [
                "client_id",
                "age",
                "job",
                "marital",
                "education",
                "credit_default",
                "mortgage",
            ]
        ]
        return pd.concat([base, proccess_client_data(client_data)], ignore_index=True)

    def proccess_campaign_data(data):
        aux_df = data.copy()
        aux_df["previous_outcome"] = aux_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
        aux_df["campaign_outcome"] = aux_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
        aux_df["last_contact_date"] = ("2022-" + aux_df["month"].astype(str) + "-" + aux_df["day"].astype(str)).astype("datetime64[ns]")
        
        aux_df = aux_df.drop(columns=["day", "month"])

        return aux_df

    def build_campaign_csv(base, data_frame):
        campaign_data = data_frame[
            [
                "client_id",
                "number_contacts",
                "contact_duration",
                "previous_campaign_contacts",
                "previous_outcome",
                "campaign_outcome",
                "day",
                "month",
            
            ]
        ]

        return pd.concat([base, proccess_campaign_data(campaign_data)], ignore_index=True)


    def proccess_economics_data(data):
        aux_df = data.copy()
        return aux_df

    def build_economics_csv(base, data_frame):
        economics_data = data_frame[
            [
                "client_id",
                "cons_price_idx",
                "euribor_three_months",
            ]
        ]

        return pd.concat([base, proccess_economics_data(economics_data)], ignore_index=True)

    for file in glob.glob("./files/input/*.csv.zip"):
        df = pd.read_csv(file, compression="zip", sep=",")

        df_clients = build_client_csv(df_clients, df)
        df_campaign = build_campaign_csv(df_campaign, df)
        df_economics = build_economics_csv(df_economics, df)

    if os.path.exists("files/output/"):
        for file in glob.glob(f"files/output/*"):
            os.remove(file)
    else:
        os.makedirs("files/output")

    df_clients.to_csv("files/output/client.csv", index=False)
    df_campaign.to_csv("files/output/campaign.csv", index=False)
    df_economics.to_csv("files/output/economics.csv", index=False)


    return


if __name__ == "__main__":
    clean_campaign_data()
