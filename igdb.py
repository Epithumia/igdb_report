import os
import json
import requests

import duckdb
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from dotenv import load_dotenv
from rich.progress import Progress
from itables import show
from pandas import DataFrame, concat


class IGDB:
    def __init__(self, csv_dir: str) -> None:
        """
        Initializes an IGBD in-memory database.

        This starts by grabbing an access token from the IGDB API,
        for which you need to have set the environment variables
        IGDB_CLIENT_ID and IGDB_CLIENT_SECRET. This code cas load
        from a .env file with the proper variables set.

        Parameters
        ----------
        csv_dir : str
            The directory where the csv files will be stored.

        """

        load_dotenv()

        self.client_id = os.getenv("IGDB_CLIENT_ID")
        self.client_secret = os.getenv("IGDB_CLIENT_SECRET")

        body = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }

        r = requests.post("https://id.twitch.tv/oauth2/token", body)

        keys = r.json()
        self.access_token = keys["access_token"]

        self.csv_dir = csv_dir
        if not os.path.exists(self.csv_dir):
            os.mkdir(self.csv_dir)

        self.endpoints = list()
        self.schema = dict()

        self.cnx = None

    def download_igdb_csv(
        self, endpoint: str, destination: str, progress: Progress
    ) -> None:
        """
        Parameters
        ----------
        endpoint : str
            The endpoint to download.
        destination : str
            The destination file.
        progress : Progress
            Reference to the progress bar context.
        """

        with open(destination, "wb") as f:
            url = endpoint["url"]
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    progress.update(endpoint["task"], advance=len(chunk))
                    f.write(chunk)
        schema = destination.replace(".csv", ".json")
        with open(schema, "w") as f:
            json.dump(endpoint["schema"], f, indent=2)

        progress.update(endpoint["task"], visible=False)

    def download_igdb_csv_dumps(self):
        """
        Downloads all the CSV dumps from the IGDB API. If the file on disk is newer then the
        API file, then it will not re-download it.
        """

        e = requests.get(
            "https://api.igdb.com/v4/dumps",
            headers={
                "Client-ID": self.client_id,
                "Authorization": f"Bearer {self.access_token}",
            },
        )

        endpoints = e.json()

        endpoint_list = list()

        for endpoint in endpoints:
            endpoint_list.append((endpoint["endpoint"], endpoint["updated_at"]))

        endpoints = dict()

        with Progress(transient=True) as progress:

            task = progress.add_task(
                "[green]Getting endpoints...", total=len(endpoint_list)
            )

            for endpoint, updated_at in endpoint_list:

                filename = endpoint + ".csv"
                destination = os.path.join(self.csv_dir, filename)
                mod_time = 0
                if os.path.exists(destination):
                    mod_time = os.path.getmtime(destination)
                if mod_time < updated_at:
                    e = requests.get(
                        f"https://api.igdb.com/v4/dumps/{endpoint}",
                        headers={
                            "Client-ID": self.client_id,
                            "Authorization": f"Bearer {self.access_token}",
                        },
                    )

                    endpoints[endpoint] = {
                        "url": e.json()["s3_url"],
                        "schema": e.json()["schema"],
                        "size": int(e.json()["size_bytes"]),
                        "updated_at": int(e.json()["updated_at"]),
                    }

                else:
                    schema_filename = endpoint + ".json"
                    schema_destination = os.path.join(self.csv_dir, schema_filename)
                    with open(schema_destination, "r") as f:
                        schema = json.load(f)
                    endpoints[endpoint] = {
                        "url": None,
                        "schema": schema,
                        "size": 0,
                        "updated_at": updated_at,
                    }

                progress.update(task, advance=1)

        with Progress(transient=True) as progress:

            with ThreadPoolExecutor(max_workers=4) as pool:
                for endpoint in endpoints:
                    filename = endpoint + ".csv"
                    destination = os.path.join(self.csv_dir, filename)
                    mod_time = 0
                    if os.path.exists(destination):
                        mod_time = os.path.getmtime(destination)
                    if mod_time < endpoints[endpoint]["updated_at"]:
                        task = progress.add_task(
                            f"[green]Downloading {endpoint}...",
                            total=endpoints[endpoint]["size"],
                        )
                        endpoints[endpoint]["task"] = task

                        pool.submit(
                            self.download_igdb_csv,
                            endpoints[endpoint],
                            destination,
                            progress,
                        )
        self.endpoints = endpoints

    def load_database(self):
        """
        Loads the csv files into the in-memory database using duckdb.
        """

        if self.endpoints is None:
            self.download_igdb_csv_dumps()

        self.cnx = duckdb.connect(database=":memory:")

        base_query = "ALTER TABLE {endpoint} ALTER COLUMN {field} TYPE {type} USING regexp_split_to_array(ltrim(rtrim({field},'}}'),'{{'),',')"

        with Progress(transient=True) as progress:

            task_preparation = progress.add_task(
                "[green] Préparation...", total=len(self.endpoints)
            )

            task_transformation = progress.add_task(
                "[green] Transformation des listes...", total=None
            )

            update_queries = list()

            # Two workarounds for fields that are not detected as LONG because they are empty.
            update_queries.append(
                "ALTER TABLE external_games ALTER COLUMN platform TYPE LONG"
            )
            update_queries.append(
                "ALTER TABLE platform_version_release_dates ALTER COLUMN platform_version TYPE LONG"
            )

            for endpoint in self.endpoints:
                filename = endpoint + ".csv"
                schema_filename = endpoint + ".json"
                csv_file = os.path.join(self.csv_dir, filename)
                json_file = os.path.join(self.csv_dir, schema_filename)
                schema = json.load(open(json_file, "r"))
                self.schema[endpoint] = schema
                for field, type in schema.items():
                    if "[]" in type:
                        update_queries.append(
                            base_query.format(endpoint=endpoint, field=field, type=type)
                        )
                update_queries.append(f"CREATE INDEX {endpoint}_idx ON {endpoint}(id)")
                self.cnx.sql(f"CREATE TABLE {endpoint} AS FROM read_csv('{csv_file}')")

                progress.update(task_preparation, advance=1)

            progress.update(task_transformation, total=len(update_queries))

            for query in update_queries:
                self.cnx.sql(query)
                progress.update(task_transformation, advance=1)

    def check_empty_endpoints(self):
        """
        Checks if the endpoints are empty (i.e. no rows in the CSV file).
        """

        no_empty_endpoints = True
        df = DataFrame(columns=["Endpoint"])
        for endpoint in self.endpoints:
            self.cnx.execute(f"SELECT COUNT(*) FROM {endpoint}")
            r = self.cnx.fetchone()
            if r[0] == 0:
                no_empty_endpoints = False
                concat(
                    [DataFrame([[endpoint]], columns=df.columns), df], ignore_index=True
                )
        if no_empty_endpoints:
            print("All endpoints are populated")
        else:
            print("The following endpoints are empty:")
            df = df.sort_values(by=["Endpoint"]).reset_index(drop=True)
            show(df, buttons=["copyHtml5", "csvHtml5"])

    def check_empty_fields(
        self, deprecated=defaultdict(lambda: None), ignored=defaultdict(lambda: None)
    ):
        """
        Goes through each endpoint, and finds the fields that are empty
        (i.e. all values are null).
        """

        self.cnx.execute(
            "SELECT table_name FROM information_schema.tables ORDER BY table_name"
        )
        res = self.cnx.fetchall()
        df = DataFrame(columns=["Endpoint", "Field"])
        for endpoint in res:
            self.cnx.execute(
                f"""
                SELECT column_name, null_percentage 
                FROM (SUMMARIZE {endpoint[0]}) s 
                WHERE null_percentage = 100.0 
                ORDER BY column_name"""
            )
            r = self.cnx.fetchall()
            empty_fields = list()
            for row in r:
                if (
                    row[0] not in deprecated[endpoint[0]]
                    and row[0] not in ignored[endpoint[0]]
                    and row[-1] == 100.0
                ):
                    empty_fields.append(row[0])
            if len(empty_fields) > 0:
                for field in empty_fields:
                    df = concat(
                        [DataFrame([[endpoint[0], field]], columns=df.columns), df],
                        ignore_index=True,
                    )

        if len(df) == 0:
            print("No empty fields")
        else:
            df = df.sort_values(by=["Endpoint", "Field"]).reset_index(drop=True)
            show(df, buttons=["copyHtml5", "csvHtml5"])

    def check_duplicate_values_in_arrays(self):
        """
        Goes through each endpoint, and finds the fields that contain
        duplicate values in arrays (for example, an event references the same game twice).
        """

        df = DataFrame(columns=["Endpoint", "Id", "Field", "Value", "Count"])
        for endpoint in self.endpoints:
            schema = self.schema[endpoint]
            for field, type in schema.items():
                if "[]" in type:
                    self.cnx.execute(
                        f"""
                        WITH CTE AS (
                        SELECT id, unnest({field}) c FROM {endpoint} where {field} IS NOT NULL) 
                        SELECT id, c, count(*) FROM CTE 
                        GROUP BY id, c 
                        HAVING count(*) > 1 
                        ORDER BY id, c"""
                    )
                    res = self.cnx.fetchall()
                    if len(res) > 0:
                        for r in res:
                            df = concat(
                                [
                                    DataFrame(
                                        [[endpoint, r[0], field, r[1], r[2]]],
                                        columns=df.columns,
                                    ),
                                    df,
                                ],
                                ignore_index=True,
                            )

        if len(df) == 0:
            print("No duplicate values in arrays")
        else:
            print("Duplicate values in arrays")
            df = df.sort_values(by=["Endpoint", "Field", "Id"]).reset_index(drop=True)
            show(df, buttons=["copyHtml5", "csvHtml5"])

    def check_broken_reference(
        self, endpoint_from, field_from, endpoint_to, field_to="id"
    ):
        """
        Goes through each endpoint, and finds the fields that contain
        broken references (for example, an event references a non-existing game).

        Parameters
        ----------
        endpoint_from : str
            The source endpoint
        field_from : str
            The source field
        endpoint_to : str
            The referenced endpoint
        field_to : str
            The referenced field, defaults to id

        Example
        -------
        To check that all events reference existing games, you would call
        check_broken_reference("events", "game", "games")
        """

        if "[]" in self.schema[endpoint_from][field_from]:
            self.cnx.execute(
                f"""
                WITH CTE AS (SELECT id, unnest({field_from}) c FROM {endpoint_from}) 
                SELECT id, c 
                FROM CTE 
                WHERE c NOT IN (
                    SELECT {field_to} 
                    FROM {endpoint_to}
                    ) 
                ORDER BY id, c"""
            )
        else:
            self.cnx.execute(
                f"""
                SELECT id, {field_from} c
                FROM {endpoint_from} 
                WHERE c NOT IN (
                    SELECT {field_to} 
                    FROM {endpoint_to}
                )
                ORDER BY id, c"""
            )

        df = DataFrame(
            columns=[
                "Endpoint (source)",
                "Id",
                "Field (source)",
                "Value",
                "Endpoint (referenced)",
            ]
        )
        res = self.cnx.fetchall()
        if len(res) > 0:
            for r in res:
                df = concat(
                    [
                        DataFrame(
                            [[endpoint_from, r[0], field_from, r[1], endpoint_to]],
                            columns=df.columns,
                        ),
                        df,
                    ],
                    ignore_index=True,
                )
            print(f"{len(df)} missing references in {endpoint_from}.{field_from}")
            df = df.sort_values(
                by=["Endpoint (source)", "Field (source)", "Id"]
            ).reset_index(drop=True)
            show(df, buttons=["copyHtml5", "csvHtml5"])
        else:
            print(f"No missing reference in {endpoint_from}.{field_from}")

    def query(self, query):
        """
        Utility function to query the database.

        Parameters
        ----------
        query : str
            The SQL query
        """

        try:
            show(self.cnx.sql(query).to_df(), buttons=["copyHtml5", "csvHtml5"])
        except AttributeError:
            pass
