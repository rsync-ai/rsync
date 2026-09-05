"""First-party cloud connectors (redshift, azure-blob) must resolve to a real
brand logo slug, not the grey placeholder.

Root cause these tests lock down: the logo downloader substitutes ONE `{icon}`
slug into every source, but the sources use different slug conventions and
several AWS/Azure/Google *product* marks were removed from Simple Icons under
trademark policy. Concretely (verified live at authoring time):

    cdn.simpleicons.org/amazonredshift        -> 404
    cdn.simpleicons.org/microsoftazure        -> 404
    cdn.simpleicons.org/googlecloudstorage    -> 200   (gcs already works)
    cdn.simpleicons.org/googlebigquery        -> 200   (bigquery already works)
    gilbarbara/logos/main/logos/aws-redshift  -> 200   (redshift lives here)
    gilbarbara/logos/main/logos/microsoft-azure -> 200 (azure lives here)

The Gil Barbara source is ALREADY in SVG_LOGO_SOURCES, so the fix is purely
slug resolution: register the brand + emit the Gil-Barbara-convention slug as
an alternate so the existing machinery hits a real mark. These are pure,
offline assertions on the resolver — no network.
"""

from src.utils.logo_downloader import (
    get_connector_info,
    _get_alternate_icon_names,
)


class TestFirstPartyCloudRegistry:
    def test_redshift_registered_with_brand_slug(self):
        info = get_connector_info("redshift")
        assert info["icon"] == "amazonredshift"
        assert "amazon" in info["domain"]

    def test_amazon_redshift_alias_registered(self):
        info = get_connector_info("amazon-redshift")
        assert info["icon"] == "amazonredshift"

    def test_azure_blob_registered_with_brand_slug(self):
        info = get_connector_info("azure-blob")
        assert info["icon"] == "microsoftazure"
        assert "azure" in info["domain"]

    def test_azure_blob_underscore_form_normalizes(self):
        # `_normalize_name` maps azure_blob -> azure-blob
        info = get_connector_info("azure_blob")
        assert info["icon"] == "microsoftazure"


class TestGilBarbaraAlternateSlugs:
    def test_redshift_alternates_include_gilbarbara_slug(self):
        alts = _get_alternate_icon_names("redshift", "amazonredshift")
        assert "aws-redshift" in alts

    def test_azure_blob_alternates_include_gilbarbara_slug(self):
        alts = _get_alternate_icon_names("azure-blob", "microsoftazure")
        assert "microsoft-azure" in alts

    def test_alternate_fires_off_primary_icon_too(self):
        # Even if the connector id differs, a known primary slug still yields
        # the Gil-Barbara alternate.
        alts = _get_alternate_icon_names("some-redshift-wrapper", "amazonredshift")
        assert "aws-redshift" in alts

    def test_curated_alternates_do_not_leak_to_unrelated_connectors(self):
        alts = _get_alternate_icon_names("stripe", "stripe")
        assert "aws-redshift" not in alts
        assert "microsoft-azure" not in alts


class TestNoRegressionOnWorkingBrands:
    def test_gcs_still_resolves_working_simpleicons_slug(self):
        assert get_connector_info("gcs")["icon"] == "googlecloudstorage"

    def test_bigquery_still_resolves_working_simpleicons_slug(self):
        assert get_connector_info("bigquery")["icon"] == "googlebigquery"
