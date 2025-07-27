import marimo

__generated_with = "0.14.13"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""# Data integrity report for IGDB""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Libraries used by the notebook""")
    return


@app.cell
def _():
    from collections import defaultdict
    from itables import init_notebook_mode
    from igdb_marimo import IGDB
    return IGDB, defaultdict


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Class IGDB""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Load data from the data dumps""")
    return


@app.cell
def _(IGDB, mo):
    igdb = IGDB("csv", mo)  # Initializes the object, the folder parameter sets where the csv files will be downloaded/loaded from. This also fetches an access token and stores it in the object.
    return (igdb,)


@app.cell
def _(igdb):
    igdb.download_igdb_csv_dumps()  # Downloads all the csv dumps. If the files on disk are more recent than the dumps available, this will skip them.
    return


@app.cell
def _(igdb):
    igdb.load_database()  # Loads them in a in-memory database
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""List all the endpoints:""")
    return


@app.cell
def _(igdb):
    igdb.query("SELECT table_name FROM information_schema.tables")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Empty endpoints

    The cell below will list all endpoints that have no rows (which means there was a sync issue)
    """
    )
    return


@app.cell
def _(igdb):
    igdb.check_empty_endpoints()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Empty fields""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Fill the two dictionaries below with deprecated fields or fields you want to ignore so they are not reported.""")
    return


@app.cell
def _(defaultdict):
    deprecated = defaultdict(lambda: list())
    ignored = defaultdict(lambda: list())

    # The following two fields in the games endpoint are marked as deprecated in the documentation.
    deprecated["age_ratings"] = ["category", "rating"]
    deprecated["age_rating_content_descriptions"] = ["category"]
    deprecated["characters"] = ["gender", "species"]
    deprecated["companies"] = ["change_date_category", "start_date_category"]
    deprecated["company_websites"] = ["category"]
    deprecated["external_games"] = ["category", "media"]
    deprecated["games"] = ["category", "collection", "follows", "status"]
    deprecated["platforms"] = ["category"]
    deprecated["platform_version_release_dates"] = ["category", "region"]
    deprecated["platform_websites"] = ["category"]
    deprecated["popularity_primitives"] = ["popularity_source"]
    deprecated["popularity_types"] = ["popularity_source"]
    deprecated["release_dates"] = ["category", "region"]
    deprecated["websites"] = ["category"]

    # Popularity primitives is special and has empty checksums
    ignored["popularity_primitives"] = ["checksum"]

    # This field isn't in the API documentation
    ignored["platform_version_release_dates"] = ["game"]
    return deprecated, ignored


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""The cell below will list for each endpoint that has at least one empty column which column is empty.""")
    return


@app.cell
def _(deprecated, igdb, ignored):
    igdb.check_empty_fields(deprecated, ignored)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Duplicate values in arrays""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""The cell below will show which endpoints have arrays that contain duplicate values.""")
    return


@app.cell
def _(igdb):
    igdb.check_duplicate_values_in_arrays()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Broken references

    This section checks that fields that reference other endpoints don't contain stale/broken data to ensure the integrity of the relations. Since the references are only listed in the api documentation and are therefore painful to automate, this is broken down by endpoint and would need updating when endpoints evolve.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Age Rating""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("age_ratings", "content_descriptions", "age_rating_content_descriptions")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("age_ratings", "organization", "age_rating_organizations")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("age_ratings", "rating_category", "age_rating_categories")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("age_ratings", "rating_content_descriptions", "age_rating_content_descriptions_v2")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Age Rating Category""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("age_rating_categories", "organization", "age_rating_organizations")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Age Rating Content Description V2""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("age_rating_content_descriptions_v2", "description_type", "age_rating_content_description_types")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("age_rating_content_descriptions_v2", "organization", "age_rating_organizations")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Alternative names""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("alternative_names", "game", "games")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Artworks""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("artworks", "artwork_type", "artwork_types")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("artworks", "game", "games")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Characters""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("characters", "character_gender", "character_genders")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("characters", "character_species", "character_species")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("characters", "games", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("characters", "mug_shot", "character_mug_shots")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Collections""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collections", "as_child_relations", "collection_relations")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collections", "as_parent_relations", "collection_relations")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collections", "games", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collections", "type", "collection_types")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Collection memberships""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collection_memberships", "collection", "collections")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collection_memberships", "game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collection_memberships", "type", "collection_membership_types")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Collection membership types""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collection_membership_types", "allowed_collection_type", "collection_types")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Collection relations""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collection_relations", "child_collection", "collections")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collection_relations", "parent_collection", "collections")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collection_relations", "type", "collection_relation_types")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Collection Relation Types""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collection_relation_types", "allowed_child_type", "collection_types")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("collection_relation_types", "allowed_parent_type", "collection_types")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Companies""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("companies", "change_date_format", "date_formats")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("companies", "changed_company_id", "companies")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("companies", "developed", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("companies", "logo", "company_logos")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("companies", "parent", "companies")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("companies", "published", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("companies", "start_date_format", "date_formats")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("companies", "status", "company_statuses")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("companies", "websites", "company_websites")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Company Website""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("company_websites", "type", "website_types")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Covers""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("covers", "game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("covers", "game_localization", "game_localizations")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Events""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("events", "event_logo", "event_logos")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("events", "event_networks", "event_networks")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("events", "games", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("events", "videos", "game_videos")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Event logos""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("event_logos", "event", "events")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Event networks""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("event_networks", "event", "events")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("event_networks", "network_type", "network_types")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### External Games""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("external_games", "external_game_source", "external_game_sources")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("external_games", "game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("external_games", "game_release_format", "game_release_formats")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("external_games", "platform", "platforms")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Franchise""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("franchises", "games", "games")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Games""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "age_ratings", "age_ratings")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "alternative_names", "alternative_names")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "artworks", "artworks")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "bundles", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "collections", "collections")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "cover", "covers")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "dlcs", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "expanded_games", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "expansions", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "external_games", "external_games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "forks", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "franchise", "franchises")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "franchises", "franchises")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "game_engines", "game_engines")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "game_localizations", "game_localizations")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "game_modes", "game_modes")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "game_status", "game_statuses")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "game_type", "game_types")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "genres", "genres")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "involved_companies", "companies")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "keywords", "keywords")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "language_supports", "language_supports")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "multiplayer_modes", "multiplayer_modes")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "parent_game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "platforms", "platforms")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "player_perspectives", "player_perspectives")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "ports", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "release_dates", "release_dates")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "remakes", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "remasters", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "screenshots", "screenshots")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "similar_games", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "standalone_expansions", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "themes", "themes")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "version_parent", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "videos", "game_videos")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("games", "websites", "websites")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Game Engines""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_engines", "companies", "companies")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_engines", "logo", "game_engine_logos")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_engines", "platforms", "platforms")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Game Localizations""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_localizations", "cover", "covers")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_localizations", "game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_localizations", "region", "regions")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Game Versions""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_versions", "features", "game_version_features")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_versions", "game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_versions", "games", "games")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Game Version Features""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_version_features", "values", "game_version_feature_values")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Game Version Feature Values""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_version_feature_values", "game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_version_feature_values", "game_feature", "game_version_features")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Game Videos""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("game_videos", "game", "games")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Involved Companies""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("involved_companies", "company", "companies")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("involved_companies", "game", "games")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Language Supports""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("language_supports", "game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("language_supports", "language", "languages")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("language_supports", "language_support_type", "language_support_types")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Multiplayer Modes""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("multiplayer_modes", "game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("multiplayer_modes", "platform", "platforms")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Network Types""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("network_types", "event_networks", "event_networks")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Platforms""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platforms", "platform_family", "platform_families")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platforms", "platform_logo", "platform_logos")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platforms", "platform_type", "platform_types")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platforms", "versions", "platform_versions")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platforms", "websites", "platform_websites")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Platform Versions""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platform_versions", "companies", "platform_version_companies")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platform_versions", "main_manufacturer", "platform_version_companies")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platform_versions", "platform_logo", "platform_logos")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platform_versions", "platform_version_release_dates", "platform_version_release_dates")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Platform Version Companies""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platform_version_companies", "company", "companies")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Platform Version Release Dates""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platform_version_release_dates", "date_format", "date_formats")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platform_version_release_dates", "platform_version", "platform_versions")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platform_version_release_dates", "release_region", "release_date_regions")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Platform Website""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("platform_websites", "type", "website_types")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Popularity Primitives""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("popularity_primitives", "external_popularity_source", "external_game_sources")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("popularity_primitives", "popularity_type", "popularity_types")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Popularity Type""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("popularity_types", "external_popularity_source", "external_game_sources")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Release Dates""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("release_dates", "date_format", "date_formats")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("release_dates", "game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("release_dates", "platform", "platforms")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("release_dates", "release_region", "release_date_regions")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("release_dates", "status", "release_date_statuses")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Screenshots""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("screenshots", "game", "games")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Websites""")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("websites", "game", "games")
    return


@app.cell
def _(igdb):
    igdb.check_broken_reference("websites", "type", "website_types")
    return


if __name__ == "__main__":
    app.run()
